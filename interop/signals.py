""":module:`server.lib.interop.signals` Interop Signals

"""

from blinker import NamedSignal
from blinker import Namespace

_signals = Namespace()

#: Runs code after the Pacemaker extension has been bootstrapped.
interop_ready: NamedSignal = _signals.signal("interop-ready")
