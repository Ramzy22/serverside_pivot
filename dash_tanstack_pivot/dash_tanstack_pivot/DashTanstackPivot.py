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


class DashTanstackPivot(Component):
    """A DashTanstackPivot component.


Keyword arguments:

- id (string; optional)

- availableFieldList (list of strings; optional)

- cellUpdate (dict; optional)

- cellUpdates (list of dicts; optional)

- colFields (list; optional)

- columnPinned (dict; optional)

- columnPinning (dict; optional)

    `columnPinning` is a dict with keys:

    - left (list of strings; optional)

    - right (list of strings; optional)

- columnSizing (dict; optional)

- columnVisibility (dict; optional)

- columns (list; optional)

- conditionalFormatting (list of dicts; optional)

- data (list of dicts; optional)

- dataOffset (number; optional)

- dataVersion (number; optional)

- defaultTheme (string; optional)

- drillEndpoint (string; optional)

- drillThrough (dict; optional)

- expanded (dict | boolean; optional)

- filterOptions (dict; optional)

- filters (dict; optional)

- grandTotalPosition (a value equal to: 'top', 'bottom'; optional)

- persistence (boolean | string | number; optional)

- persistence_type (a value equal to: 'local', 'session', 'memory'; optional)

- pinningOptions (dict; optional)

    `pinningOptions` is a dict with keys:

    - maxPinnedLeft (number; optional)

    - maxPinnedRight (number; optional)

    - suppressMovable (boolean; optional)

    - lockPinned (boolean; optional)

- pinningPresets (list of dicts; optional)

    `pinningPresets` is a list of dicts with keys:

    - name (string; optional)

    - config (dict; optional)

- pivotTitle (string; optional)

- reset (boolean | number | string | dict | list; optional)

- rowCount (number; optional)

- rowFields (list; optional)

- rowMove (dict; optional)

- rowPinned (dict; optional)

- rowPinning (dict; optional)

    `rowPinning` is a dict with keys:

    - top (list of strings; optional)

    - bottom (list of strings; optional)

- saveViewTrigger (boolean | number | string | dict | list; optional)

- savedView (dict; optional)

- serverSide (boolean; optional)

- showColTotals (boolean; optional)

- showRowTotals (boolean; optional)

- sortEvent (dict; optional)

- sortLock (boolean; optional)

- sortOptions (dict; optional)

    `sortOptions` is a dict with keys:

    - naturalSort (boolean; optional)

    - caseSensitive (boolean; optional)

    - columnOptions (dict; optional)

- sorting (list; optional)

- table (string; optional)

- valConfigs (list of dicts; optional)

    `valConfigs` is a list of dicts with keys:

    - field (string; optional)

    - agg (a value equal to: 'sum', 'avg', 'count', 'min', 'max', 'weighted_avg', 'wavg', 'weighted_mean'; optional)

    - weightField (string; optional)

    - windowFn (a value equal to: 'percent_of_row', 'percent_of_col', 'percent_of_grand_total'; optional)

    - format (string; optional)

    - percentile (number; optional)

    - separator (string; optional)

- validationRules (dict; optional)

- viewState (dict; optional)

- viewport (dict; optional)"""
    _children_props = []
    _base_nodes = ['children']
    _namespace = 'dash_tanstack_pivot'
    _type = 'DashTanstackPivot'
    ValConfigs = TypedDict(
        "ValConfigs",
            {
            "field": NotRequired[str],
            "agg": NotRequired[Literal["sum", "avg", "count", "min", "max", "weighted_avg", "wavg", "weighted_mean"]],
            "weightField": NotRequired[str],
            "windowFn": NotRequired[Literal["percent_of_row", "percent_of_col", "percent_of_grand_total"]],
            "format": NotRequired[str],
            "percentile": NotRequired[NumberType],
            "separator": NotRequired[str]
        }
    )

    ColumnPinning = TypedDict(
        "ColumnPinning",
            {
            "left": NotRequired[typing.Sequence[str]],
            "right": NotRequired[typing.Sequence[str]]
        }
    )

    RowPinning = TypedDict(
        "RowPinning",
            {
            "top": NotRequired[typing.Sequence[str]],
            "bottom": NotRequired[typing.Sequence[str]]
        }
    )

    PinningOptions = TypedDict(
        "PinningOptions",
            {
            "maxPinnedLeft": NotRequired[NumberType],
            "maxPinnedRight": NotRequired[NumberType],
            "suppressMovable": NotRequired[bool],
            "lockPinned": NotRequired[bool]
        }
    )

    PinningPresets = TypedDict(
        "PinningPresets",
            {
            "name": NotRequired[str],
            "config": NotRequired[dict]
        }
    )

    SortOptions = TypedDict(
        "SortOptions",
            {
            "naturalSort": NotRequired[bool],
            "caseSensitive": NotRequired[bool],
            "columnOptions": NotRequired[dict]
        }
    )


    def __init__(
        self,
        id: typing.Optional[typing.Union[str, dict]] = None,
        table: typing.Optional[str] = None,
        pivotTitle: typing.Optional[str] = None,
        data: typing.Optional[typing.Sequence[dict]] = None,
        style: typing.Optional[typing.Any] = None,
        serverSide: typing.Optional[bool] = None,
        rowCount: typing.Optional[NumberType] = None,
        rowFields: typing.Optional[typing.Sequence] = None,
        colFields: typing.Optional[typing.Sequence] = None,
        valConfigs: typing.Optional[typing.Sequence["ValConfigs"]] = None,
        filters: typing.Optional[dict] = None,
        sorting: typing.Optional[typing.Sequence] = None,
        expanded: typing.Optional[typing.Union[dict, bool]] = None,
        columns: typing.Optional[typing.Sequence] = None,
        showRowTotals: typing.Optional[bool] = None,
        showColTotals: typing.Optional[bool] = None,
        grandTotalPosition: typing.Optional[Literal["top", "bottom"]] = None,
        filterOptions: typing.Optional[dict] = None,
        viewport: typing.Optional[dict] = None,
        cellUpdate: typing.Optional[dict] = None,
        cellUpdates: typing.Optional[typing.Sequence[dict]] = None,
        rowMove: typing.Optional[dict] = None,
        drillThrough: typing.Optional[dict] = None,
        drillEndpoint: typing.Optional[str] = None,
        viewState: typing.Optional[dict] = None,
        saveViewTrigger: typing.Optional[typing.Any] = None,
        savedView: typing.Optional[dict] = None,
        conditionalFormatting: typing.Optional[typing.Sequence[dict]] = None,
        validationRules: typing.Optional[dict] = None,
        columnPinning: typing.Optional["ColumnPinning"] = None,
        rowPinning: typing.Optional["RowPinning"] = None,
        columnPinned: typing.Optional[dict] = None,
        rowPinned: typing.Optional[dict] = None,
        columnVisibility: typing.Optional[dict] = None,
        columnSizing: typing.Optional[dict] = None,
        reset: typing.Optional[typing.Any] = None,
        persistence: typing.Optional[typing.Union[bool, str, NumberType]] = None,
        persistence_type: typing.Optional[Literal["local", "session", "memory"]] = None,
        pinningOptions: typing.Optional["PinningOptions"] = None,
        pinningPresets: typing.Optional[typing.Sequence["PinningPresets"]] = None,
        sortOptions: typing.Optional["SortOptions"] = None,
        sortLock: typing.Optional[bool] = None,
        defaultTheme: typing.Optional[str] = None,
        sortEvent: typing.Optional[dict] = None,
        availableFieldList: typing.Optional[typing.Sequence[str]] = None,
        dataOffset: typing.Optional[NumberType] = None,
        dataVersion: typing.Optional[NumberType] = None,
        **kwargs
    ):
        self._prop_names = ['id', 'availableFieldList', 'cellUpdate', 'cellUpdates', 'colFields', 'columnPinned', 'columnPinning', 'columnSizing', 'columnVisibility', 'columns', 'conditionalFormatting', 'data', 'dataOffset', 'dataVersion', 'defaultTheme', 'drillEndpoint', 'drillThrough', 'expanded', 'filterOptions', 'filters', 'grandTotalPosition', 'persistence', 'persistence_type', 'pinningOptions', 'pinningPresets', 'pivotTitle', 'reset', 'rowCount', 'rowFields', 'rowMove', 'rowPinned', 'rowPinning', 'saveViewTrigger', 'savedView', 'serverSide', 'showColTotals', 'showRowTotals', 'sortEvent', 'sortLock', 'sortOptions', 'sorting', 'style', 'table', 'valConfigs', 'validationRules', 'viewState', 'viewport']
        self._valid_wildcard_attributes =            []
        self.available_properties = ['id', 'availableFieldList', 'cellUpdate', 'cellUpdates', 'colFields', 'columnPinned', 'columnPinning', 'columnSizing', 'columnVisibility', 'columns', 'conditionalFormatting', 'data', 'dataOffset', 'dataVersion', 'defaultTheme', 'drillEndpoint', 'drillThrough', 'expanded', 'filterOptions', 'filters', 'grandTotalPosition', 'persistence', 'persistence_type', 'pinningOptions', 'pinningPresets', 'pivotTitle', 'reset', 'rowCount', 'rowFields', 'rowMove', 'rowPinned', 'rowPinning', 'saveViewTrigger', 'savedView', 'serverSide', 'showColTotals', 'showRowTotals', 'sortEvent', 'sortLock', 'sortOptions', 'sorting', 'style', 'table', 'valConfigs', 'validationRules', 'viewState', 'viewport']
        self.available_wildcard_properties =            []
        _explicit_args = kwargs.pop('_explicit_args')
        _locals = locals()
        _locals.update(kwargs)  # For wildcard attrs and excess named props
        args = {k: _locals[k] for k in _explicit_args}

        super(DashTanstackPivot, self).__init__(**args)

setattr(DashTanstackPivot, "__init__", _explicitize_args(DashTanstackPivot.__init__))
