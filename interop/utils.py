
import time
import typing
import uuid
from asyncio import Event
from enum import Enum
from functools import total_ordering

from .signals import interop_ready

_interop: typing.Any = None


@interop_ready.connect
def _set_interop(sender):
    """Sets the global interop instance for the module"""

    global _interop
    _interop = sender


class Exchanges(Enum):
    APPLICATION = "application"
    #: Active interop instances
    HEARTBEAT = "heartbeat"
    #: Process log records
    LOG = "log"
    #: Data manipulation - create, delete, soft delete, update
    #: Learners connect to this exchange
    #: Permission indexers connect to this exchange
    NOTIFY = "notify"
    PREDICT = "predict"
    #: Track query execution - entities associated with req.
    #: Learners connect to this exchange
    QUERY = "query"
    #: Track does not store request json and response rather dumps them on
    #: this exchange - limited timeout though and does not persist
    #: Learners connect to this exchange
    TRACK = "track"


class ExchangeTypes(Enum):
    DIRECT = "direct"
    FANOUT = "fanout"
    HEADERS = "headers"
    TOPIC = "topic"


@total_ordering
class Packet:
    """A packet contains all that is necessary to transmit through the broker.
    """

    __slots__ = (
        "app",
        "exchange",
        "routing_key",
        "wait",
        "content_type",
        "expiration",
        "referrer",
        "delivery_mode",
        "_data",
        "timestamp",
        "correlation_id",
        "reply_exchange",
        "reply_routing_key",
    )

    def __init__(
        self,
        exchange: str,
        routing_key: str = "",
        content_type: str = "application/octet-stream",
        expiration: int = 5,
        referrer: str = None,
        delivery_mode: int = 2,
    ):

        self.content_type = content_type
        self.exchange = exchange
        self.routing_key = routing_key

        #: 2 ensures packet persistion on rmq
        self.delivery_mode = delivery_mode
        self.expiration = 1000 * expiration
        self.timestamp = time.time()
        self.referrer = referrer

        #: For RPC's
        self.correlation_id = None
        self.reply_exchange = None
        self.reply_routing_key = None

    @property
    def data(self):
        return getattr(self, "_data", None)

    @data.setter
    def data(self, value):
        """The actual data sent to the exchange. Data must be json / dict
        format.
        """

        if value is None or not any(
            type(value) == x for x in [dict, list, set, tuple]
        ):

            raise RuntimeError("Value must be a dict, list, set or tuple!!")
        #: Ensure type conformity as this will have to be pickled
        setattr(self, "_data", value)

    def __eq__(self, other):
        return id(self) < id(other)

    def __lt__(self, other):
        return id(self) == id(other)

    def __repr__(self):
        return f"<Packet bound for {self.exchange}>"


@total_ordering
class Priorities(Enum):
    VERY_LOW = 10
    LOW = 8
    NORMAL = 6
    HIGH = 4
    VERY_HIGH = 2
    NOW = 0

    def __lt__(self, other):
        """In priority queues, lower numbers have higher priority."""

        return self.value > other.value


def low(
    data,
    exchange: str,
    routing_key: str,
    expiration: int = 5,
    delivery_mode: int = 2,
):
    """Packets and enqueues data of low priority."""

    packet = Packet(
        exchange,
        routing_key=routing_key,
        expiration=expiration,
        delivery_mode=delivery_mode,
    )
    packet.data = data

    try:
        _interop.publisher_queue.put_nowait((Priorities.LOW.value, packet))
    except:  # noqa
        pass


def normal(
    data,
    exchange: str,
    routing_key: str,
    expiration: int = 5,
    delivery_mode: int = 2,
):
    """Packets and enqueues data of normal priority."""

    packet = Packet(
        exchange,
        routing_key=routing_key,
        expiration=expiration,
        delivery_mode=delivery_mode,
    )
    packet.data = data

    try:
        _interop.publisher_queue.put_nowait((Priorities.NORMAL.value, packet))
    except:  # noqa
        pass


def now(
    data,
    exchange: str,
    routing_key: str,
    expiration: int = 5,
    delivery_mode: int = 2,
):
    """Packets and enqueues data of now priority."""

    packet = Packet(
        exchange,
        routing_key=routing_key,
        expiration=expiration,
        delivery_mode=delivery_mode,
    )
    packet.data = data

    try:
        _interop.publisher_queue.put_nowait((Priorities.NOW.value, packet))
    except:  # noqa
        pass


def high(
    data,
    exchange: str,
    routing_key: str,
    expiration: int = 5,
    delivery_mode: int = 2,
):
    """Packets and enqueues data of high priority."""

    packet = Packet(
        exchange,
        routing_key=routing_key,
        expiration=expiration,
        delivery_mode=delivery_mode,
    )
    packet.data = data

    try:
        _interop.publisher_queue.put_nowait((Priorities.HIGH.value, packet))
    except:  # noqa
        pass


def rpc(
    data,
    exchange: str,
    routing_key: str,
    expiration: int = 5,
    delivery_mode: int = 2,
) -> typing.Tuple[str, Event]:

    """Package and queue the packet."""

    packet = Packet(
        exchange,
        routing_key=routing_key,
        expiration=expiration,
        delivery_mode=delivery_mode,
    )
    packet.data = data

    #: Set RPC routing parameters
    packet.correlation_id = uuid.uuid4().hex  # type: ignore
    packet.reply_exchange = Exchanges.APPLICATION.value  # type: ignore

    #: Return Routing Key
    rrk = f"{_interop.name}.{_interop._thread}.rpc"
    packet.reply_routing_key = rrk  # type: ignore

    #: Keep track of pending RPC
    #: Each interop listens for an RPC result push to the application
    #: name.thread.rpc combination and then uses the correlation id to get and
    #: set the notification event that the result is ready, after which the
    #: result is posted to the `_rpcs_returned` attribute.
    rpc_event = Event()
    _interop._rpcs_pending.update({packet.correlation_id: rpc_event})

    try:
        _interop.publisher_queue.put_nowait((Priorities.NOW.value, packet))
    except:  # noqa
        pass
    return packet.correlation_id, rpc_event


def reply(_packet: Packet):
    """Packages and returns the data in the packet.

    When a subscriber handles an RPC call, it is passed the packet. It reads
    the data and responds by modifying the data. Calling reply here would
    prep and publish the response based on passed packet.

    """

    if (
        _packet.correlation_id
        and _packet.reply_exchange
        and _packet.reply_routing_key
    ):

        packet = Packet(
            _packet.reply_exchange,
            routing_key=_packet.reply_routing_key,
            expiration=5,
            delivery_mode=_packet.delivery_mode,
        )
        packet.correlation_id = _packet.correlation_id
        packet.data = _packet.data

        try:
            _interop.publisher_queue.put_nowait((Priorities.NOW.value, packet))
        except:  # noqa
            pass


def very_high(
    data,
    exchange: str,
    routing_key: str,
    expiration: int = 5,
    delivery_mode: int = 2,
):
    """Packets and enqueues data of very high priority."""

    packet = Packet(
        exchange,
        routing_key=routing_key,
        expiration=expiration,
        delivery_mode=delivery_mode,
    )
    packet.data = data

    try:
        _interop.publisher_queue.put_nowait(
            (Priorities.VERY_HIGH.value, packet)
        )
    except:  # noqa
        pass


def very_low(
    data,
    exchange: str,
    routing_key: str,
    expiration: int = 5,
    delivery_mode: int = 2,
):
    """Packets and enqueues data of very low priority."""

    packet = Packet(
        exchange,
        routing_key=routing_key,
        expiration=expiration,
        delivery_mode=delivery_mode,
    )
    packet.data = data

    try:
        _interop.publisher_queue.put_nowait(
            (Priorities.VERY_LOW.value, packet)
        )
    except:  # noqa
        pass
