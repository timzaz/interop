""":module:`server.lib.interop.utils` Broker Utilities

"""

import errno
import os
import sys
import time
import types
import typing
import uuid
from asyncio import Event
from enum import Enum
from functools import total_ordering

from .signals import interop_ready

_interop: typing.Any = None


@interop_ready.connect
async def _set_interop(sender):
    """Sets the global interop instance for the module"""

    global _interop
    _interop = sender


class Config(dict):
    """Works exactly like a dict but provides ways to fill it from files
    or special dictionaries.  There are two common patterns to populate the
    config.

    Either you can fill the config from a config file::

        config.from_pyfile('yourconfig.cfg')

    Or alternatively you can define the configuration options in the
    module that calls :meth:`from_object` or provide an import path to
    a module that should be loaded.  It is also possible to tell it to
    use the same module and with that provide the configuration values
    just before the call::

        DEBUG = True
        SECRET_KEY = 'development key'
        config.from_object(__name__)

    In both cases (loading from any Python file or loading from modules),
    only uppercase keys are added to the config.  This makes it possible to use
    lowercase values in the config file for temporary values that are not added
    to the config or to define the config keys in the same file that implements
    the application.

    Probably the most interesting way to load configurations is from an
    environment variable pointing to a file::

        config.from_envvar('YOURAPPLICATION_SETTINGS')

    In this case before launching the application you have to set this
    environment variable to the file you want to use.  On Linux and OS X
    use the export statement::

        export YOURAPPLICATION_SETTINGS='/path/to/config/file'

    On windows use `set` instead.

    :param root_path: absolute path to which files are read relative from.
    :param defaults: an optional dictionary of default values
    """

    def __init__(
        self,
        *,
        root_path: str,
        defaults: typing.Dict[str, typing.Any] = None
    ):
        dict.__init__(self, defaults or {})
        self.root_path = root_path

    def casted(
        self,
        *,
        default: typing.Any = None,
        func: typing.Callable = None,
        key: str
    ) -> typing.Any:
        """Casts the value of config's key with func if specified.

        """

        if key in self:
            value = self[key]
        elif default is not None:
            value = default
        else:
            raise KeyError(f"Config '{key}' is missing, and has no default.")

        if func is None or value is None:
            return value
        elif func is bool and isinstance(value, str):
            mapping = {
                "0": False,
                "1": True,
                "false": False,
                "n": False,
                "no": False,
                "true": True,
                "y": True,
                "yes": True
            }

            value = value.lower()
            if value not in mapping:
                raise ValueError(
                    f"Config '{key}' has value '{value}'. Not a valid bool."
                )
            return mapping[value]

        try:
            return func(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"Config '{key}' has value '{value}'. "
                f"Not a valid {func.__name__}."
            )

    def from_envvar(
        self,
        *,
        silent: bool = False,
        variable_name: str
    ) -> bool:
        """Loads a configuration from an environment variable pointing to
        a configuration file.  This is basically just a shortcut with nicer
        error messages for this line of code::

            config.from_pyfile(os.environ['YOURAPPLICATION_SETTINGS'])

        :param variable_name: name of the environment variable
        :param silent: set to ``True`` if you want silent failure for missing
                       files.
        :return: bool. ``True`` if able to load config, ``False`` otherwise.
        """
        rv = os.environ.get(variable_name)
        if not rv:
            if silent:
                return False
            raise RuntimeError(
                f"The environment variable {variable_name!r} is not set"
                " and as such configuration could not be loaded. Set"
                " this variable and make it point to a configuration"
                " file"
            )
        return self.from_pyfile(filename=rv, silent=silent)

    def from_file(
        self,
        *,
        filename: str,
        load: typing.Callable,
        silent: bool = False
    ) -> bool:
        """Update the values in the config from a file that is loaded
        using the ``load`` parameter. The loaded data is passed to the
        :meth:`from_mapping` method.

        .. code-block:: python

            import toml
            config.from_file("config.toml", load=toml.load)

        :param filename: The path to the data file. This can be an
            absolute path or relative to the config root path.
        :param load: A callable that takes a file handle and returns a
            mapping of loaded data from the file.
        :type load: ``Callable[[Reader], Mapping]`` where ``Reader``
            implements a ``read`` method.
        :param silent: Ignore the file if it doesn't exist.

        """
        filename = os.path.join(self.root_path, filename)

        try:
            with open(filename) as f:
                obj = load(f)
        except OSError as e:
            if silent and e.errno in (errno.ENOENT, errno.EISDIR):
                return False

            e.strerror = f"Unable to load configuration file ({e.strerror})"
            raise

        return self.from_mapping(obj)

    def get_namespace(
        self,
        *,
        namespace: str,
        lowercase: bool = True,
        trim_namespace: bool = True
    ) -> typing.Dict[str, typing.Any]:
        """Returns a dictionary containing a subset of configuration options
        that match the specified namespace/prefix. Example usage::

            config['IMAGE_STORE_TYPE'] = 'fs'
            config['IMAGE_STORE_PATH'] = '/var/app/images'
            config['IMAGE_STORE_BASE_URL'] = 'http://img.website.com'
            image_store_config = config.get_namespace('IMAGE_STORE_')

        The resulting dictionary `image_store_config` would look like::

            {
                'type': 'fs',
                'path': '/var/app/images',
                'base_url': 'http://img.website.com'
            }

        This is often useful when configuration options map directly to
        keyword arguments in functions or class constructors.

        :param namespace: a configuration namespace
        :param lowercase: a flag indicating if the keys of the resulting
                          dictionary should be lowercase
        :param trim_namespace: a flag indicating if the keys of the resulting
                          dictionary should not include the namespace

        """
        rv = {}
        for k, v in self.items():
            if not k.startswith(namespace):
                continue
            if trim_namespace:
                key = k[len(namespace):]
            else:
                key = k
            if lowercase:
                key = key.lower()
            rv[key] = v
        return rv

    def from_mapping(self, *mapping, **kwargs) -> bool:
        """Updates the config like :meth:`update` ignoring items with non-upper
        keys.

        """

        mappings = []
        if len(mapping) == 1:
            if hasattr(mapping[0], "items"):
                mappings.append(mapping[0].items())
            else:
                mappings.append(mapping[0])
        elif len(mapping) > 1:
            raise TypeError(
                f"expected at most 1 positional argument, got {len(mapping)}"
            )
        mappings.append(kwargs.items())
        for mapping in mappings:
            for (key, value) in mapping:
                if key.isupper():
                    self[key] = value
        return True

    def from_pyfile(self, *, filename: str, silent: bool = False) -> bool:
        """Updates the values in the config from a Python file.  This function
        behaves as if the file was imported as module with the
        :meth:`from_object` function.

        :param filename: the filename of the config.  This can either be an
                         absolute filename or a filename relative to the
                         root path.
        :param silent: set to ``True`` if you want silent failure for missing
                       files.
        """
        filename = os.path.join(self.root_path, filename)
        d = types.ModuleType("config")
        d.__file__ = filename
        try:
            with open(filename, mode="rb") as config_file:
                exec(compile(config_file.read(), filename, "exec"), d.__dict__)
        except OSError as e:
            if (
                silent and
                e.errno in (
                    errno.ENOENT,
                    errno.EISDIR,
                    errno.ENOTDIR
                )
            ):
                return False
            e.strerror = f"Unable to load configuration file ({e.strerror})"
            raise
        self.from_object(d)
        return True

    def from_object(self, obj: typing.Union[object, str]) -> None:
        """Updates the values from the given object.  An object can be of one
        of the following two types:

        -   a string: in this case the object with that name will be imported
        -   an actual object reference: that object is used directly

        Objects are usually either modules or classes. :meth:`from_object`
        loads only the uppercase attributes of the module/class. A ``dict``
        object will not work with :meth:`from_object` because the keys of a
        ``dict`` are not attributes of the ``dict`` class.

        Example of module-based configuration::

            app.config.from_object('yourapplication.default_config')
            from yourapplication import default_config
            app.config.from_object(default_config)

        Nothing is done to the object before loading. If the object is a
        class and has ``@property`` attributes, it needs to be
        instantiated before being passed to this method.

        You should not use this function to load the actual configuration but
        rather configuration defaults.  The actual config should be loaded
        with :meth:`from_pyfile` and ideally from a location not within the
        package because the package might be installed system wide.

        See :ref:`config-dev-prod` for an example of class-based configuration
        using :meth:`from_object`.

        :param obj: an import name or object
        """
        if isinstance(obj, str):
            obj = import_string(obj)
        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)

    def __repr__(self):
        return f"<{type(self).__name__} {dict.__repr__(self)}>"


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


class ImportStringError(ImportError):
    """Provides information about a failed :func:`import_string` attempt."""

    #: String in dotted notation that failed to be imported.
    import_name = None
    #: Wrapped exception.
    exception = None

    def __init__(self, import_name, exception):
        self.import_name = import_name
        self.exception = exception

        msg = (
            "import_string() failed for %r. Possible reasons are:\n\n"
            "- missing __init__.py in a package;\n"
            "- package or module path not included in sys.path;\n"
            "- duplicated package or module name taking precedence in "
            "sys.path;\n"
            "- missing module, class, function or variable;\n\n"
            "Debugged import:\n\n%s\n\n"
            "Original exception:\n\n%s: %s"
        )

        name = ""
        tracked = []
        for part in import_name.replace(":", ".").split("."):
            name += (name and ".") + part
            imported = import_string(name, silent=True)
            if imported:
                tracked.append((name, getattr(imported, "__file__", None)))
            else:
                track = ["- %r found in %r." % (n, i) for n, i in tracked]
                track.append("- %r not found." % name)
                msg = msg % (
                    import_name,
                    "\n".join(track),
                    exception.__class__.__name__,
                    str(exception),
                )
                break

        ImportError.__init__(self, msg)

    def __repr__(self):
        return "<%s(%r, %r)>" % (
            self.__class__.__name__,
            self.import_name,
            self.exception,
        )


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


def import_string(import_name, silent=False):
    """Imports an object based on a string.  This is useful if you want to
    use import paths as endpoints or something similar.  An import path can
    be specified either in dotted notation (``xml.sax.saxutils.escape``)
    or with a colon as object delimiter (``xml.sax.saxutils:escape``).

    If `silent` is True the return value will be `None` if the import fails.

    :param import_name: the dotted name for the object to import.
    :param silent: if set to `True` import errors are ignored and
                   `None` is returned instead.
    :return: imported object
    """
    # force the import name to automatically convert to strings
    # __import__ is not able to handle unicode strings in the fromlist
    # if the module is a package
    import_name = str(import_name).replace(":", ".")
    try:
        try:
            __import__(import_name)
        except ImportError:
            if "." not in import_name:
                raise
        else:
            return sys.modules[import_name]

        module_name, obj_name = import_name.rsplit(".", 1)
        module = __import__(module_name, globals(), locals(), [obj_name])
        try:
            return getattr(module, obj_name)
        except AttributeError as e:
            raise ImportError(e)

    except ImportError as e:
        if not silent:
            reraise(
                ImportStringError,
                ImportStringError(import_name, e),
                sys.exc_info()[2]
            )


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


def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


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
