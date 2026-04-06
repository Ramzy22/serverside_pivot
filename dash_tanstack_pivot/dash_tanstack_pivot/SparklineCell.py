# AUTO GENERATED FILE - DO NOT EDIT

import typing  # noqa: F401
from typing_extensions import TypedDict, NotRequired, Literal # noqa: F401
from dash.development.base_component import Component, _explicitize_args

ComponentType = typing.Union[
    str,
    int,
    float,
    Component,
    None,
    typing.Sequence[typing.Union[str, int, float, Component, None]],
]

NumberType = typing.Union[
    typing.SupportsFloat, typing.SupportsInt, typing.SupportsComplex
]


class SparklineCell(Component):
    """A SparklineCell component.


Keyword arguments:
"""
    _children_props = []
    _base_nodes = ['children']
    _namespace = 'dash_tanstack_pivot'
    _type = 'SparklineCell'


    def __init__(
        self,
        points = None,
        type = None,
        color = None,
        positiveColor = None,
        negativeColor = None,
        areaOpacity = None,
        showCurrentValue = None,
        showDelta = None,
        currentLabel = None,
        deltaLabel = None,
        title = None,
        compact = None,
        **kwargs
    ):
        self._prop_names = []
        self._valid_wildcard_attributes =            []
        self.available_properties = []
        self.available_wildcard_properties =            []
        _explicit_args = kwargs.pop('_explicit_args')
        _locals = locals()
        _locals.update(kwargs)  # For wildcard attrs and excess named props
        args = {k: _locals[k] for k in _explicit_args}

        super(SparklineCell, self).__init__(**args)

setattr(SparklineCell, "__init__", _explicitize_args(SparklineCell.__init__))
