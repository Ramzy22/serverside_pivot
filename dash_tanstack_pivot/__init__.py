import copy as _copy

from .dash_tanstack_pivot import *  # noqa: F401,F403
from .dash_tanstack_pivot import (  # noqa: F401
    DashTanstackPivot,
    __all__ as _inner_all,
    __version__,
    _css_dist as _inner_css_dist,
    _js_dist as _inner_js_dist,
)


def _prefix_relative_path(entry):
    next_entry = _copy.deepcopy(entry)
    relative = str(next_entry.get("relative_package_path", ""))
    next_entry["relative_package_path"] = f"dash_tanstack_pivot/{relative}"
    return next_entry


_js_dist = [_prefix_relative_path(entry) for entry in _inner_js_dist]
_css_dist = [_prefix_relative_path(entry) for entry in _inner_css_dist]

for _component_name in _inner_all:
    _component = globals().get(_component_name)
    if _component is None:
        continue
    setattr(_component, "_js_dist", _js_dist)
    setattr(_component, "_css_dist", _css_dist)
