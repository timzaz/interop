
import pickle
import typing
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import PriorityQueue
from asyncio.queues import QueueEmpty
from logging import Logger

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel

from .utils import Packet


class Publisher:
    """This is simple publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    .. note::
      The publisher has to subscribe to all exchanges. Items to be published
      are put on a queue that this publisher constantly queries and publishes.
      All information for publishing are included in the queue item - defined
      as a :class:`~phase.messenger.Packet`.

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
        """Setup the publisher object, passing in the URL we will use
        to connect to RabbitMQ.

        :param str amqp_url: The URL for connecting to RabbitMQ
        :param dict exchange_map: The exchanges this publisher will connect to.
          This is a tuple of the exchange name and type. Exchanges

        """

        self._acked: int = 0
        self._connection: typing.Optional[AsyncioConnection] = None
        self._channel: typing.Optional[Channel] = None
        self._custom_loop = custom_loop

        self._deliveries: typing.Optional[typing.List[int]] = None
        self._exchange_ok_count: int = 0

        self._exchanges: typing.List[typing.Tuple[str, str]] = exchanges
        self._message_number: int = 0
        self._nacked: int = 0

        self._name: str = f"Publisher instance {thread} of app {name}"
        self._reply_to: str = f"{name}.{thread}"

        self._stopping: bool = False
        self._url: str = amqp_url

    def _can_publish(self):
        """Indicates whether the publisher instance is in a position to publish
        messages. This is usually the case when all exchanges have been
        declared alright.

        """

        return self._exchange_ok_count == len(self._exchanges)

    def _connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.

        :rtype: pika.SelectConnection

        """

        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        return AsyncioConnection(
            pika.URLParameters(self._url),
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
            custom_ioloop=self._custom_loop,
        )

    def _declare_exchanges(self):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """

        #: Declare all exchanges by default
        for e in self._exchanges:
            typing.cast(Channel, self._channel).exchange_declare(
                callback=self._on_exchange_declareok,
                exchange=e[0],
                exchange_type=e[1],
            )

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        :param pika.channel.Channel channel: The channel object

        """

        self._channel = channel
        #: This method tells pika to call the on_channel_closed method if
        #: RabbitMQ unexpectedly closes the channel.
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._declare_exchanges()

    def _on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """

        self._channel = None
        if not self._stopping:
            typing.cast(AsyncioConnection, self._connection).ioloop.call_later(
                5, self._reconnect
            )

    def _on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param str reason: The text reason the channel was closed

        """

        self._channel = None
        if not self._stopping:
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

        #: This method will open a new channel with RabbitMQ by issuing the
        #: Channel.Open RPC command. When RabbitMQ confirms the channel is open
        #: by sending the Channel.OpenOK RPC reply, the on_channel_open method
        #: will be invoked.
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

    def _on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response
        frame

        """

        self._exchange_ok_count += 1
        if self._can_publish():
            #: Enable delivery confirmations and schedule first message
            #: Send the Confirm.Select RPC method to RabbitMQ to enable
            #: delivery confirmations on the channel. The only way to turn this
            #: off is to close the channel and create a new one.

            #: When the message is confirmed from RabbitMQ, the
            #: on_delivery_confirmation method will be invoked passing in a
            #: Basic.Ack or Basic.Nack method from RabbitMQ that will indicate
            #: which messages it is confirming or rejecting.
            typing.cast(Channel, self._channel).confirm_delivery(
                self._on_delivery_confirmation
            )
            self._schedule_next_message()

    def _on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """

        confirmation_type = method_frame.method.NAME.split(".")[1].lower()
        # t = method_frame.method.delivery_tag
        # self._info(
        #   f'{self._name} received {confirmation_type} for delivery tag: {t}')

        if confirmation_type == "ack":
            self._acked += 1
        elif confirmation_type == "nack":
            self._nacked += 1

        #: TODO: Requeue the unacknowledged packet data
        typing.cast(typing.List[int], self._deliveries).remove(
            method_frame.method.delivery_tag
        )

    def _publish_message(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """

        if self._channel is None or not self._channel.is_open:
            return

        #: Wait for next message
        try:
            _, packet = self._queue.get_nowait()
        except QueueEmpty:
            return self._schedule_next_message()

        if not isinstance(packet, Packet):
            return self._schedule_next_message()

        #: This should not be confused for the routing key for this application
        #: just the instance thread that is publishing the request
        packet.referrer = self._name
        hdrs = {
            "correlation_id": packet.correlation_id,
            "exchange": packet.exchange,
            "routing_key": packet.routing_key,
            "referrer": packet.referrer,
        }

        #: Ensure RP has a way to communicate back
        if (
            packet.reply_exchange
            and packet.reply_routing_key
            and packet.correlation_id
        ):
            hdrs.update(
                {
                    "reply_exchange": packet.reply_exchange,
                    #: Always reply to publishing phase
                    "reply_routing_key": packet.reply_routing_key,
                }
            )

        try:
            content_type = "application/octet-stream"
            data = pickle.dumps(packet.data)
            properties = pika.BasicProperties(
                app_id=self._name,
                content_type=content_type,
                expiration=str(packet.expiration),
                headers=hdrs,
            )

            self._channel.basic_publish(
                packet.exchange, packet.routing_key, data, properties
            )

            #: TODO: Future code improvement
            #: Add all packets sent to deliveries list
            #: On acknowledgement, remove from it.
            #: On 3 retries, remove from it.
            self._message_number += 1
            typing.cast(typing.List[int], self._deliveries).append(
                self._message_number
            )
        except Exception as exc:
            #: Example: Publishing to undeclared exchange
            #: Could be a couple of reasons, e.g. Exchange not bound to
            self._error(f"{self._name} could not broker packet: {exc}")

        #: Immediately run the next available message
        self._schedule_next_message()

    def _reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """

        if not self._stopping:
            #: Create a new connection
            self._connection = self._connect()

    def _schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """

        typing.cast(AsyncioConnection, self._connection).ioloop.call_later(
            0.000001, self._publish_message
        )

    async def run(self, logger: Logger, queue: PriorityQueue, event: Event):
        """Run the example code by connecting and then starting the IOLoop.

        :params:`queue` The queue that the publisher will receive packets from
        :params:`event` The event to be set when the publisher connects

        """

        self._error = logger.error
        self._event = event
        self._info = logger.info
        self._queue = queue
        self._warning = logger.warning

        self._connection = self._connect()

    def stop(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """

        self._stopping = True
        if self._channel is not None:
            self._channel.close()

        if self._connection is not None:
            self._connection.close()
