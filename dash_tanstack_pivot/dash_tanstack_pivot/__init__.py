from __future__ import print_function as _

import os as _os
import sys as _sys
import json

__version__ = None

import dash as _dash

# noinspection PyUnresolvedReferences
from ._imports_ import *
from ._imports_ import __all__ as _generated_all
from .DashTanstackPivot import DashTanstackPivot

__all__ = [*list(_generated_all), 'DashTanstackPivot']

if not hasattr(_dash, '__version__'):
    raise TypeError('Dash requires code to be generated')

_basepath = _os.path.dirname(__file__)
_filepath = _os.path.abspath(_os.path.join(_basepath, 'package-info.json'))

with open(_filepath) as f:
    package = json.load(f)

package_name = package['name'].replace(' ', '_').replace('-', '_')
__version__ = package['version']

_current_path = _os.path.dirname(_os.path.abspath(__file__))

_this_module = _sys.modules[__name__]

def _package_external_url(filename):
    return 'https://unpkg.com/{}/@{}/{}/{}'.format(
        package_name,
        __version__,
        package_name,
        filename,
    )


def _discover_async_js_dist():
    chunk_suffix = '.{}.min.js'.format(package_name)
    main_bundle = '{}.min.js'.format(package_name)
    try:
        filenames = sorted(_os.listdir(_current_path))
    except OSError:
        return []
    return [
        {
            'relative_package_path': filename,
            'external_url': _package_external_url(filename),
            'namespace': package_name,
            'dynamic': True,
        }
        for filename in filenames
        if filename.endswith(chunk_suffix) and filename != main_bundle
    ]


_js_dist = [
    {
        'relative_package_path': '{}.min.js'.format(package_name),
        'external_url': _package_external_url('{}.min.js'.format(package_name)),
        'namespace': package_name
    },
    {
        'relative_package_path': '{}.min.js.map'.format(package_name),
        'external_url': _package_external_url('{}.min.js.map'.format(package_name)),
        'namespace': package_name,
        'dynamic': True
    },
    *_discover_async_js_dist(),
]

_css_dist = []


for _component in __all__:
    setattr(locals()[_component], '_js_dist', _js_dist)
    setattr(locals()[_component], '_css_dist', _css_dist)
