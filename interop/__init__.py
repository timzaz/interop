from __future__ import annotations

import hashlib
import logging
import os
import re
import socket
import threading
import typing
from asyncio import Event
from asyncio import PriorityQueue
from asyncio import create_task
from asyncio import get_event_loop
from asyncio import iscoroutinefunction
from functools import cached_property
from functools import update_wrapper
from functools import wraps
from uuid import uuid4

from .publisher import Publisher
from .signals import interop_ready
from .subscriber import Subscriber
from .utils import Exchanges
from .utils import ExchangeTypes
from .utils import Packet
from .utils import Priorities
from .utils import high
from .utils import low
from .utils import normal
from .utils import now
from .utils import very_high
from .utils import very_low

__all__ = (
    "Publisher",
    "Subscriber",
    "Exchanges",
    "ExchangeTypes",
    "Packet",
    "Priorities",
    "high",
    "low",
    "normal",
    "now",
    "very_high",
    "very_low",
    "Interop",
)

__deffered_publishers: typing.List[
    typing.Callable[[typing.Dict[str, typing.Any]], typing.Coroutine]
] = list()

__deffered_subscribers: typing.List[
    typing.Tuple[str, str, typing.Callable[[Packet], typing.Coroutine]]
] = list()

default_config: typing.Dict[str, typing.Any] = {
    "IMPORT_NAME": __name__,
    "RMQ_BROKER_URI": (
        "amqp://guest:guest@localhost:5672/%2F?"
        "connection_attempts=3&heartbeat=3600"
    ),
}

default_exchanges: typing.List[typing.Tuple[str, str]] = [
    (
        e.value,
        ExchangeTypes.TOPIC.value
        if e != Exchanges.HEARTBEAT
        else ExchangeTypes.FANOUT.value,
    )
    for e in Exchanges
]


def ack(interop: Interop):
    """The function just acknowledges this interoperable instance is active."""

    async def _ack(packet):
        #: Respond by putting on queue
        packet.data = {
            "alive": True,
            "iam": f"{interop.name}_{interop.instance}: {interop._thread}",
        }

    return _ack


def before_connect(f):
    """Ensures handlers and publishs are specified before the interop is
    connected to the RMQ server.

    """

    @wraps(f)
    def wrapper_func(self, *args, **kwargs):
        if self._connected:
            raise AssertionError(
                "This publish / handler must be specified before connecting "
                "the app to the broker."
            )
        return f(self, *args, **kwargs)

    return wrapper_func


def publish(
    func: typing.Callable[[typing.Dict[str, typing.Any]], typing.Coroutine],
) -> typing.Callable[
    [typing.Callable[[typing.Dict[str, typing.Any]], typing.Coroutine]],
    typing.Any,
]:
    """Registers a publisher.

    >>> @publish()
    >>> async def monitor_weather(app: typing.Dict[str, typing.Any]):
    >>>     ...
    """

    __deffered_publishers.append(func)
    update_wrapper(publish, func)

    return publish


@interop_ready.connect
def register_deferred(sender: Interop):
    global __deffered_publishers
    global __deffered_subscribers

    try:
        while True:
            publisher = __deffered_publishers.pop(0)
            sender.publish(publisher)
    except IndexError:
        pass

    try:
        while True:
            (routing_key, exchange, subscriber) = __deffered_subscribers.pop(0)
            sender.subscribe(routing_key, exchange, subscriber)
    except IndexError:
        pass

    del __deffered_publishers
    del __deffered_subscribers


def rpc_result(interop: Interop):
    """The result of all RPC's that this interoperable pushes out are
    handled by this function first.

    When an RPC is made an event with a unique identifier is generated. This
    unique identifier is the correlation id. The calling function will wait for
    this event to set at a particular time. This function picks out the
    correlation id (unique identifier) from the recieved packet
    and set the event so the calling function can continue.

    A task could be created that waits until the event is set for the
    correlation id and then pulls the returned data to carry on its processing.
    There is no way for us to determine when the RPC is actually returned.
    The app may have restarted in the background and the correlation id cache
    of expecting results may have been flushed.

    """

    async def _rpc_result(packet):
        rpc_event = interop._rpcs_pending.get(packet.correlation_id)
        if rpc_event:
            interop._rpcs_returned[packet.correlation_id] = packet.data
            rpc_event.set()

    return _rpc_result


def subscribe(
    routing_key: str,
    exchange: str,
) -> typing.Callable[
    [typing.Callable[[Packet], typing.Coroutine]], typing.Any
]:
    """Registers a subscriber.

    >>> @subscribe("route", "exchange")
    >>> async def send_sms(packet: Packet):
    >>>     ...
    """

    def inner_subscribe(func: typing.Callable[[Packet], typing.Coroutine]):
        """Decorates an actual subscriber."""

        __deffered_subscribers.append((routing_key, exchange, func))
        update_wrapper(inner_subscribe, func)

    return inner_subscribe


class Interop:
    """An interoperable script. One which has both publisher and subscriber
    running as coroutines.

    ..note::
      The Subscriber can hold as many handlers as possible - up to the devs to
      decide what's `possible` for a particular project.
      An Interop may hold as many publishers too. Publishers are basically
      observables, configured to carry out some actions when certain conditions
      are met. The possibility for race conditions is very high. Use with
      extreme caution!

    """

    def __init__(
        self,
        name: str,
        uri: str,
        *,
        debug: bool = False,
        exchanges: typing.List[typing.Tuple[str, str]] = default_exchanges,
    ):
        """Initialise the interoperable.

        :param name: The name of the interoperable
        :param exchanges: A tuple of exchange name, exchange type pair.

        """

        self.broker_uri = uri
        assert self.broker_uri is not None, "RMQ_BROKER_URI must not be None."

        self._connected: bool = False
        self._debug = debug
        self._publishers: typing.Set[
            typing.Callable[[typing.Dict[str, typing.Any]], typing.Coroutine]
        ] = set()
        self._publisher_started = Event()
        self._rpcs_pending: typing.Dict[str, Event] = dict()
        self._rpcs_returned: typing.Dict[str, typing.Any] = dict()
        self._subscriber_handlers = dict()
        self._subscriber_started = Event()
        self._thread = threading.current_thread().ident
        self._threads_started = False

        self.exchanges = exchanges
        self.futures = list()
        self.name = name
        #: Create the publisher queue and make it accessible to publishers.
        #: Do not force confirmation of actual connection to RMQ to identify
        #: as connected - connected simply means all the handlers and publishs
        #: have been defined for the interop.
        #: However, ensure the queue maxes out so as not to cause memory issues
        #: TODO: Figure out the ideal max size
        self.publisher_queue = PriorityQueue(1000000)

    async def _connect(self):
        """Start the tasks."""

        #: Subscribe to the Ack method
        self.subscribe(
            f"{self.name}.ack",
            Exchanges.HEARTBEAT.value,
            ack(self),
        )

        #: Subscribe to the RPC result method
        #: This handler receives the sent RPC's response
        self.subscribe(
            f"{self.name}.{self._thread}.{uuid4().hex}.rpc",
            Exchanges.APPLICATION.value,
            rpc_result(self),
        )

        #: Create subscriber task
        self.futures.append(
            create_task(
                self.subscriber.run(
                    logging.getLogger(f"{self.name}.subscriber"),
                    self._subscriber_started,
                    self._subscriber_handlers,
                    self.app,
                )
            )
        )

        #: Create subscriber executor task
        self.futures.append(create_task(self.subscriber.executor()))

        #: Create publisher task
        self.futures.append(
            create_task(
                self.publisher.run(
                    logging.getLogger(f"{self.name}.publisher"),
                    self.publisher_queue,
                    self._subscriber_started,
                )
            )
        )

        #: Set the indicator flag
        self._connected = True

        #: Accept and manipulate the data.
        for f in self._publishers:
            self.futures.append(create_task(f(self.app)))

    async def init_app(
        self,
        *,
        app: typing.Dict[str, typing.Any] = default_config,
    ):
        """Initialise."""

        self.app = app

        #: Signal to bootstrap all cogs that have been brought into the
        #: execution context.
        interop_ready.send(self)

        loop = get_event_loop()
        self.publisher = Publisher(
            self.name, self.broker_uri, self.exchanges, self._thread, loop
        )
        self.subscriber = Subscriber(
            self.name, self.broker_uri, self.exchanges, self._thread, loop
        )

        app["interop"] = self
        await self._connect()

    @cached_property
    def instance(self):
        """The interop instance is a combination of the dirname of this file,
        the virtualenv dirname and the host name.
        """

        name = f"{self.servername}::{self.virtualenv}::{self.name}"
        _instance = hashlib.md5(
            hashlib.md5(bytes(name, "utf-8")).digest()
        ).digest()
        return _instance

    @before_connect
    def publish(
        self,
        f: typing.Callable[[typing.Dict[str, typing.Any]], typing.Coroutine],
    ):
        """A callable that accepts 1 parameter that is run in COG mode as the
        primary function.

        For example: A publish could be created just to capture and log health
        information of a server system or to come up with suggestions or update
        system information such as generic information on location boundaries.

        """

        if not iscoroutinefunction(f):
            raise ValueError(
                "Cruncher expected value to be an asynchronous callable."
            )
        self._publishers.add(f)

    def servername(self):
        """Returns the server name these files are on."""

        return socket.gethostname()

    @before_connect
    def subscribe(
        self,
        routing_key: str,
        exchange: str,
        handler: typing.Callable[[Packet], typing.Coroutine],
    ):
        """Connects a routing key on an exchange to a handler function.

        Basically this example::

            async def file_logger(packet):
                pass


            app.add_broker_rule("log.critical", "log", file_logger)

        ..note::
          All handler functions must accept a single parameter: The Packet
          which contains all the information the handler should need to process
          the message.

        """

        #: Only valid exchanges
        exchanges = [e[0] for e in self.exchanges]
        assert exchange.lower() in exchanges, "Invalid exchange specified!"

        #: Enforce exchange restrictions

        #: routing_key must be specified in RMQ format
        #: it should then be made into regex format because the subscriber
        #: has to pattern match a message received from RMQ to select the
        #: actual handler to consume the message.
        #: log.critical
        #: log.#
        rk = (
            routing_key.replace(
                #: First make the dots regex compatible
                ".",
                "\\.",
            )
            .replace(
                #: Then do the one word wild card
                "*",
                "[A-Za-z0-9-]+",
            )
            .replace(
                #: Then the multiple word wild card
                "#",
                "[A-Za-z0-9-\\.]+",
            )
        )
        rk_pattern = re.compile(rk)

        #: TODO: Also ensure the func accepts 1 argument of type ``Packet``
        if not iscoroutinefunction(handler):
            raise ValueError("Handler should be an asynchronous callable.")

        queue_name = handler.__name__
        endpoint = (exchange, rk_pattern, routing_key, queue_name)

        old_func = self._subscriber_handlers.get(endpoint)
        if old_func is not None and old_func != handler:
            raise AssertionError(
                "Subscriber is overwriting an existing subscriber: {endpoint}"
            )
        #: The handler that would run would be the one for the endpoint
        #: whose regex matches first for the exchange - supplied in Packet
        self._subscriber_handlers[endpoint] = handler

    @property
    def virtualenv(self):
        """The virtualenv the code is run from."""

        return os.getenv("VIRTUAL_ENV", "system_env")

    async def __call__(self, *args, **kwargs):
        """The interop is called to be run when in standalone mode. In ASGI
        mode, there is no need to call the interop as simply awaiting
        ``init_app`` will do.

        """

        if not self._connected:
            raise AssertionError(
                "This interoperable app has not connected to any broker. "
                "Be sure to call ``init_app`` first."
            )

        for f in self.futures:
            await f

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name}>"
