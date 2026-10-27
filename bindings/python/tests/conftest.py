"""The tests import the binding of this checkout (bindings/python), which loads zig-out's library."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
