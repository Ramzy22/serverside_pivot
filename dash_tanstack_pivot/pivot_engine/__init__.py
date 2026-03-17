"""
Bootstrap shim: pivot_engine outer wrapper.

The real package lives in pivot_engine/pivot_engine/. By setting __path__ to
point there, all absolute imports (from pivot_engine.types import ...) resolve
correctly, and executing the inner __init__.py makes all public names available
at the top level.
"""
import os as _os

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_INNER = _os.path.join(_HERE, "pivot_engine")

# Redirect subpackage lookups to the inner package directory.
__path__ = [_INNER]

# Execute the inner __init__.py in this namespace so all exports land here.
_inner_init = _os.path.join(_INNER, "__init__.py")
with open(_inner_init) as _f:
    exec(compile(_f.read(), _inner_init, "exec"), globals())  # noqa: S102
