""":module:`server.lib.interop.subscriber` RMQ Subscriber

"""

import pickle
import typing
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import Queue
from asyncio import QueueEmpty
from asyncio import QueueFull
from asyncio import sleep
from functools import partial
from logging import Logger

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel

from server.lib.utils import Config

from .utils import Packet
from .utils import reply


class Subscriber:
    """This is a simple consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(
        self,
        name: str,
        amqp_url: str,
        #: Exchange/ExchangeType tuple
        exchanges: typing.List[typing.Tuple[str, str]],
        thread: typing.Optional[int],
        custom_loop: typing.Optional[AbstractEventLoop] = None,
    ):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """

        self._connection: typing.Optional[AsyncioConnection] = None
        self._channel: typing.Optional[Channel] = None
        self._closing: bool = False
        self._consumer_tags: typing.List[str] = []
        self._custom_loop = custom_loop

        self._exchanges: typing.List[typing.Tuple[str, str]] = exchanges
        self._executor_queue: Queue = Queue()
        self._name: str = f"Subscriber instance {thread} of app {name}"
        self._reply_to: str = f"{name}.{thread}"
        self._thread: typing.Optional[int] = thread

        self._url: str = amqp_url

    def __call__(self, queue):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """

        def _call(channel=None, method=None, properties=None, body=None):
            #: Get the delivery tag to confirm message receipt
            dt: int = method.delivery_tag
            packet = self._rebuild_packet(properties, body)
            packet.app = self._app  # type: ignore
            handler = self._handlers.get(self._queues.get(queue))

            try:
                self._executor_queue.put_nowait(
                    _Subscription(dt, packet, handler)  # type: ignore
                )
            except QueueFull:
                self._deacknowledge_message(dt)

        return _call

    def _acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """

        typing.cast(Channel, self._channel).basic_ack(delivery_tag)

    def _connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """

        self._info(f"{self._name} connecting to {self._url}")
        return AsyncioConnection(
            pika.URLParameters(self._url),
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
            custom_ioloop=self._custom_loop,
        )

    def _deacknowledge_message(self, delivery_tag):
        """De-Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Nack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """

        typing.cast(Channel, self._channel).basic_nack(delivery_tag)

    def _declare_exchanges(self):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        """

        #: The handlers object is passed
        #: Grap the keys, and for each one get the exchange and bind it
        for k in self._handlers:
            exchange = k[0]
            type_ = [e[1] for e in self._exchanges if e[0] == exchange][0]

            on_exchange_declareok = partial(self._on_exchange_declareok, key=k)
            typing.cast(Channel, self._channel).exchange_declare(
                exchange, exchange_type=type_, callback=on_exchange_declareok
            )

    #: Multiple binds to multiple exchanges
    #: lol
    def _declare_queue(self, key):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param tuple key: The name of the queue to declare.
            (exchange, routing_key_pattern, routing_key, queue_name)
        """

        exchange = key[0]
        rk = key[2]
        queue = f"{exchange}::{rk}"  #: Convert to unicode?

        try:
            #: If queue_name specified, append it
            queue = queue + key[3]
        except IndexError:
            pass

        self._queues.update({queue: key})
        queue_declareok = partial(
            self._on_queue_declareok, queue=queue, exchange=exchange, rk=rk
        )

        #: If the calling thread is in the routing key, then this queue should
        #: be exclusive
        exclusive = str(self._thread) in rk
        typing.cast(Channel, self._channel).queue_declare(
            queue,
            durable=(not exclusive),
            exclusive=exclusive,
            callback=queue_declareok,
        )

    def _on_bindok(self, unused_frame, queue):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """

        callback = self(queue)
        tag = typing.cast(Channel, self._channel).basic_consume(
            queue, on_message_callback=callback
        )
        self._consumer_tags.append(tag)

    def _on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """

        if len(self._consumer_tags) < 1:
            typing.cast(Channel, self._channel).close()

    def _on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param str reason: The text reason the channel was closed

        """

        self._warning(
            f"{self._name}'s channel was closed: ({channel}) {reason}"
        )

        if not (
            typing.cast(AsyncioConnection, self._connection).is_closing
            or typing.cast(AsyncioConnection, self._connection).is_closed
        ):
            typing.cast(AsyncioConnection, self._connection).close()

    def _on_channel_open(self, channel: Channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """

        self._channel = channel
        # self._channel.basic_qos(10)

        #: This method tells pika to call the on_channel_closed method if
        #: RabbitMQ unexpectedly closes the channel.
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._declare_exchanges()

    def _on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param str reason: The text reason the channel was closed

        """

        self._channel = None
        if not self._closing:
            self._warning(
                f"Connection closed, reopening in 5 seconds: {reason}"
            )
            typing.cast(AsyncioConnection, self._connection).ioloop.call_later(
                5, self._reconnect
            )

    def _on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """

        #: Signal to parent thread that the connection has been made
        if self._event:
            self._event.set()

        #: Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        #: command. When RabbitMQ responds that the channel is open, the
        #: on_channel_open callback will be invoked by pika.
        typing.cast(AsyncioConnection, self._connection).channel(
            on_open_callback=self._on_channel_open
        )

    def _on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """

        self._error(f"Connection open failed, reopening in 5 seconds: {err}")
        typing.cast(AsyncioConnection, self._connection).ioloop.call_later(
            5, self._reconnect
        )

    def _on_exchange_declareok(self, unused_frame, key):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response
        frame

        """

        self._declare_queue(key)

    def _on_queue_declareok(self, method_frame, queue, exchange, rk):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """

        bindok = partial(self._on_bindok, queue=queue)
        typing.cast(Channel, self._channel).queue_bind(
            queue, exchange, rk, callback=bindok
        )

    def _rebuild_packet(self, properties, data):
        """Recomposes and returns a packet - for easier handling - for the
        passed properties and data.

        """

        #: Data needs to be loaded or something
        headers = properties.headers

        referrer = headers.get("referrer")
        exchange = headers.get("exchange")
        routing_key = headers.get("routing_key")
        reply_exchange = headers.get("reply_exchange")
        reply_routing_key = headers.get("reply_routing_key")
        correlation_id = headers.get("correlation_id")
        _data = pickle.loads(data)

        packet = Packet(
            exchange, routing_key, content_type=properties.content_type
        )
        packet.data = _data

        packet.referrer = referrer
        packet.correlation_id = correlation_id
        packet.reply_exchange = reply_exchange
        packet.reply_routing_key = reply_routing_key
        packet.delivery_mode = properties.delivery_mode
        packet.expiration = properties.expiration

        return packet

    def _reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """

        if not self._closing:
            #: Create a new connection
            self._connection = self._connect()

    def _reject_message(self, delivery_tag):
        """Reject the message delivery from RabbitMQ by sending a
        Basic.Reject RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """

        typing.cast(Channel, self._channel).basic_reject(delivery_tag)

    async def executor(self):
        """Basic Consume callbacks cannot be async. Build packet, deduce
        handlers, get delivery tag and dump in a queue, allowing this
        executor to poll the queue and execute the relevant handlers in
        async mode.

        Handlers have to be async.
        """

        while True:

            try:
                subscription: _Subscription = self._executor_queue.get_nowait()
            except QueueEmpty:
                await sleep(1)
                continue

            try:

                await subscription.handler(subscription.packet)

            except Exception as exc:
                if self._app.get("DEBUG", False):
                    import traceback

                    traceback.print_exc()

                self._error(f"Exception occured processing packet: {exc}")

                #: In case creating the context/handling the packet errors
                #: out
                #: {
                #:     'error': True,
                #:     'message': 'Reason for failure.'
                #: }
                data = {"error": True}
                if hasattr(exc, "message"):
                    data["message"] = getattr(exc, "message")

                subscription.packet.data = data

            finally:
                reply(subscription.packet)
                self._acknowledge_message(subscription.delivery_tag)

            await sleep(0)

    async def run(
        self,
        logger: Logger,
        event: Event,
        handlers: typing.Dict[typing.Any, typing.Callable[..., typing.Any]],
        app: Config,
    ):
        """Run the simple consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """

        self._app = app
        self._error = logger.error
        self._event = event
        self._handlers = handlers
        self._info = logger.info
        self._queues = {}
        self._warning = logger.warning

        self._connection = self._connect()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """

        if not self._closing:
            self._closing = True

            # if self._consuming:
            #: Tell RabbitMQ that you would like to stop consuming by
            #: sending the Basic.Cancel RPC command.
            if self._channel:
                for tag in self._consumer_tags:
                    self._channel.basic_cancel(tag, self._on_cancelok)


class _Subscription(object):
    """The data shape that gets queued to be executed by the Subscriber's
    executor.

    """

    __slots__ = (
        "delivery_tag",
        "packet",
        "handler",
    )

    def __init__(self, delivery_tag: int, packet: Packet, handler: typing.Any):
        """Initialise the subscription"""

        self.delivery_tag = delivery_tag
        self.packet = packet
        self.handler = handler
