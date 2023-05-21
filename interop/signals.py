from asyncblink import NamedAsyncSignal
from asyncblink import Namespace

_signals = Namespace()

#: Runs code after the Pacemaker extension has been bootstrapped.
interop_ready: NamedAsyncSignal = _signals.signal("interop-ready")
