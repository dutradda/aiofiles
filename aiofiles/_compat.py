import sys

try:
    from functools import singledispatch
except ImportError:                            # pragma: nocover
    from singledispatch import singledispatch

PY_35 = sys.version_info >= (3, 5)
PY_36 = sys.version_info >= (3, 6)
