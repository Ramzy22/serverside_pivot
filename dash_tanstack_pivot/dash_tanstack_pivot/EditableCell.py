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


class EditableCell(Component):
    """An EditableCell component.


Keyword arguments:
"""
    _children_props = []
    _base_nodes = ['children']
    _namespace = 'dash_tanstack_pivot'
    _type = 'EditableCell'


    def __init__(
        self,
        editingDisabled = None,
        editingDisabledReason = None,
        rowEditMode = None,
        editorConfig = None,
        rowEditSession = None,
        editorOptions = None,
        editorOptionsLoading = None,
        editorOptionsError = None,
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

        super(EditableCell, self).__init__(**args)

setattr(EditableCell, "__init__", _explicitize_args(EditableCell.__init__))
