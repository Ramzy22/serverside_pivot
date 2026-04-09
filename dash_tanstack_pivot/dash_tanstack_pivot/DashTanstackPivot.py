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

- activeReportId (string; optional)

- availableFieldList (list of strings; optional)

- cellUpdate (dict; optional)

- cellUpdates (list of dicts; optional)

- chartCanvasPanes (list of dicts; optional)

- chartDefaults (dict; optional)

- chartDefinitions (list of dicts; optional)

- chartEvent (dict; optional)

- chartServerWindow (dict; optional)

    `chartServerWindow` is a dict with keys:

    - enabled (boolean; optional)

    - rows (number; optional)

    - columns (number; optional)

    - scope (a value equal to: 'viewport', 'root'; optional)

- immersiveMode (boolean; optional)

- colFields (list; optional)

- columnPinned (dict; optional)

- columnPinning (dict; optional)

    `columnPinning` is a dict with keys:

    - left (list of strings; optional)

    - right (list of strings; optional)

- columnSizing (dict; optional)

- columnVisibility (dict; optional)

- conditionalFormatting (list of dicts; optional)

- data (list of dicts; optional)

- decimalPlaces (number; optional)

- defaultTheme (string; optional)

- defaultValueFormat (string; optional)

- detailConfig (dict; optional)

- detailMode (a value equal to: 'none', 'inline', 'sidepanel', 'drawer'; optional)

- drillEndpoint (string; optional)

- editLifecycleEvent (dict; optional)

- editState (dict; optional):
    Persisted edit state: edited cell markers, comparison original
    values, display mode,  and propagation event log. Round-tripped
    via setProps so the Dash backend can  save/restore/modify it.

- editingConfig (dict; optional)

- expanded (dict | boolean; optional)

- fieldPanelSizes (dict; optional)

- filters (dict; optional)

- grandTotalPosition (a value equal to: 'top', 'bottom'; optional)

- numberGroupSeparator (a value equal to: 'comma', 'space', 'thin_space', 'apostrophe', 'none'; optional)

- performanceConfig (dict; optional)

    `performanceConfig` is a dict with keys:

    - cacheBlockSize (number; optional)

    - maxBlocksInCache (number; optional)

    - blockLoadDebounceMs (number; optional)

    - rowOverscan (number; optional)

    - columnOverscan (number; optional)

    - prefetchColumns (number; optional)

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

- reportDef (dict; optional)

- reset (boolean | number | string | dict | list; optional)

- rowFields (list; optional)

- rowMove (dict; optional)

- rowPinned (dict; optional)

- rowPinning (dict; optional)

    `rowPinning` is a dict with keys:

    - top (list of strings; optional)

    - bottom (list of strings; optional)

- runtimeRequest (dict; optional)

- runtimeResponse (dict; optional)

- saveViewTrigger (boolean | number | string | dict | list; optional)

- savedReports (list of dicts; optional)

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

- tableCanvasSize (number; optional)

- treeConfig (dict; optional)

- uiConfig (dict; optional)

- paginationConfig (dict; optional):
    Client-side pagination configuration: { enabled: true, pageSize: 50 }.
    Only applies when serverSide is false.

- valConfigs (list of dicts; optional)

    `valConfigs` is a list of dicts with keys:

    - field (string; optional)

    - agg (a value equal to: 'sum', 'avg', 'count', 'min', 'max', 'weighted_avg', 'wavg', 'weighted_mean', 'formula'; optional)

    - weightField (string; optional)

    - windowFn (a value equal to: 'percent_of_row', 'percent_of_col', 'percent_of_grand_total'; optional)

    - format (string; optional)

    - percentile (number; optional)

    - separator (string; optional)

    - formula (string; optional)

    - label (string; optional)

    - formulaRef (string; optional)

    - sparkline (boolean | dict; optional)

- validationRules (dict; optional)

- viewMode (a value equal to: 'pivot', 'report', 'tree', 'table'; optional)

- viewState (dict; optional)"""
    _children_props = []
    _base_nodes = ['children']
    _namespace = 'dash_tanstack_pivot'
    _type = 'DashTanstackPivot'
    ValConfigs = TypedDict(
        "ValConfigs",
            {
            "field": NotRequired[str],
            "agg": NotRequired[Literal["sum", "avg", "count", "min", "max", "weighted_avg", "wavg", "weighted_mean", "formula"]],
            "weightField": NotRequired[str],
            "windowFn": NotRequired[Literal["percent_of_row", "percent_of_col", "percent_of_grand_total"]],
            "format": NotRequired[str],
            "percentile": NotRequired[NumberType],
            "separator": NotRequired[str],
            "formula": NotRequired[str],
            "label": NotRequired[str],
            "formulaRef": NotRequired[str],
            "sparkline": NotRequired[typing.Union[bool, dict]]
        }
    )

    PerformanceConfig = TypedDict(
        "PerformanceConfig",
            {
            "cacheBlockSize": NotRequired[NumberType],
            "maxBlocksInCache": NotRequired[NumberType],
            "blockLoadDebounceMs": NotRequired[NumberType],
            "rowOverscan": NotRequired[NumberType],
            "columnOverscan": NotRequired[NumberType],
            "prefetchColumns": NotRequired[NumberType]
        }
    )

    ChartServerWindow = TypedDict(
        "ChartServerWindow",
            {
            "enabled": NotRequired[bool],
            "rows": NotRequired[NumberType],
            "columns": NotRequired[NumberType],
            "scope": NotRequired[Literal["viewport", "root"]]
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
        rowFields: typing.Optional[typing.Sequence] = None,
        colFields: typing.Optional[typing.Sequence] = None,
        valConfigs: typing.Optional[typing.Sequence["ValConfigs"]] = None,
        filters: typing.Optional[dict] = None,
        sorting: typing.Optional[typing.Sequence] = None,
        expanded: typing.Optional[typing.Union[dict, bool]] = None,
        immersiveMode: typing.Optional[bool] = None,
        showRowTotals: typing.Optional[bool] = None,
        showColTotals: typing.Optional[bool] = None,
        grandTotalPosition: typing.Optional[Literal["top", "bottom"]] = None,
        runtimeRequest: typing.Optional[dict] = None,
        runtimeResponse: typing.Optional[dict] = None,
        viewMode: typing.Optional[Literal["pivot", "report", "tree", "table"]] = None,
        detailMode: typing.Optional[Literal["none", "inline", "sidepanel", "drawer"]] = None,
        treeConfig: typing.Optional[dict] = None,
        detailConfig: typing.Optional[dict] = None,
        chartEvent: typing.Optional[dict] = None,
        chartDefinitions: typing.Optional[typing.Sequence[dict]] = None,
        chartDefaults: typing.Optional[dict] = None,
        chartCanvasPanes: typing.Optional[typing.Sequence[dict]] = None,
        tableCanvasSize: typing.Optional[NumberType] = None,
        performanceConfig: typing.Optional["PerformanceConfig"] = None,
        chartServerWindow: typing.Optional["ChartServerWindow"] = None,
        cellUpdate: typing.Optional[dict] = None,
        cellUpdates: typing.Optional[typing.Sequence[dict]] = None,
        rowMove: typing.Optional[dict] = None,
        drillEndpoint: typing.Optional[str] = None,
        viewState: typing.Optional[dict] = None,
        saveViewTrigger: typing.Optional[typing.Any] = None,
        savedView: typing.Optional[dict] = None,
        conditionalFormatting: typing.Optional[typing.Sequence[dict]] = None,
        validationRules: typing.Optional[dict] = None,
        editingConfig: typing.Optional[dict] = None,
        editLifecycleEvent: typing.Optional[dict] = None,
        editState: typing.Optional[dict] = None,
        uiConfig: typing.Optional[dict] = None,
        paginationConfig: typing.Optional[dict] = None,
        columnPinning: typing.Optional["ColumnPinning"] = None,
        rowPinning: typing.Optional["RowPinning"] = None,
        columnPinned: typing.Optional[dict] = None,
        rowPinned: typing.Optional[dict] = None,
        columnVisibility: typing.Optional[dict] = None,
        columnSizing: typing.Optional[dict] = None,
        decimalPlaces: typing.Optional[NumberType] = None,
        defaultValueFormat: typing.Optional[str] = None,
        fieldPanelSizes: typing.Optional[dict] = None,
        numberGroupSeparator: typing.Optional[Literal["comma", "space", "thin_space", "apostrophe", "none"]] = None,
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
        reportDef: typing.Optional[dict] = None,
        savedReports: typing.Optional[typing.Sequence[dict]] = None,
        activeReportId: typing.Optional[str] = None,
        **kwargs
    ):
        self._prop_names = ['id', 'activeReportId', 'availableFieldList', 'cellUpdate', 'cellUpdates', 'chartCanvasPanes', 'chartDefaults', 'chartDefinitions', 'chartEvent', 'chartServerWindow', 'immersiveMode', 'colFields', 'columnPinned', 'columnPinning', 'columnSizing', 'columnVisibility', 'conditionalFormatting', 'data', 'decimalPlaces', 'defaultTheme', 'defaultValueFormat', 'detailConfig', 'detailMode', 'drillEndpoint', 'editLifecycleEvent', 'editState', 'editingConfig', 'expanded', 'fieldPanelSizes', 'filters', 'grandTotalPosition', 'numberGroupSeparator', 'performanceConfig', 'persistence', 'persistence_type', 'pinningOptions', 'pinningPresets', 'pivotTitle', 'reportDef', 'reset', 'rowFields', 'rowMove', 'rowPinned', 'rowPinning', 'runtimeRequest', 'runtimeResponse', 'saveViewTrigger', 'savedReports', 'savedView', 'serverSide', 'showColTotals', 'showRowTotals', 'sortEvent', 'sortLock', 'sortOptions', 'sorting', 'style', 'table', 'tableCanvasSize', 'treeConfig', 'paginationConfig', 'uiConfig', 'valConfigs', 'validationRules', 'viewMode', 'viewState']
        self._valid_wildcard_attributes =            []
        self.available_properties = ['id', 'activeReportId', 'availableFieldList', 'cellUpdate', 'cellUpdates', 'chartCanvasPanes', 'chartDefaults', 'chartDefinitions', 'chartEvent', 'chartServerWindow', 'immersiveMode', 'colFields', 'columnPinned', 'columnPinning', 'columnSizing', 'columnVisibility', 'conditionalFormatting', 'data', 'decimalPlaces', 'defaultTheme', 'defaultValueFormat', 'detailConfig', 'detailMode', 'drillEndpoint', 'editLifecycleEvent', 'editState', 'editingConfig', 'expanded', 'fieldPanelSizes', 'filters', 'grandTotalPosition', 'numberGroupSeparator', 'performanceConfig', 'persistence', 'persistence_type', 'pinningOptions', 'pinningPresets', 'pivotTitle', 'reportDef', 'reset', 'rowFields', 'rowMove', 'rowPinned', 'rowPinning', 'runtimeRequest', 'runtimeResponse', 'saveViewTrigger', 'savedReports', 'savedView', 'serverSide', 'showColTotals', 'showRowTotals', 'sortEvent', 'sortLock', 'sortOptions', 'sorting', 'style', 'table', 'tableCanvasSize', 'treeConfig', 'paginationConfig', 'uiConfig', 'valConfigs', 'validationRules', 'viewMode', 'viewState']
        self.available_wildcard_properties =            []
        _explicit_args = kwargs.pop('_explicit_args')
        _locals = locals()
        _locals.update(kwargs)  # For wildcard attrs and excess named props
        args = {k: _locals[k] for k in _explicit_args}

        super(DashTanstackPivot, self).__init__(**args)

setattr(DashTanstackPivot, "__init__", _explicitize_args(DashTanstackPivot.__init__))
