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
Dash public component and typed runtime composition root.

Keyword arguments:

- id (string; default PropTypes.string)

- activeReportId (string; default PropTypes.string)

- autoSizeStrategy (string; default PropTypes.string)

- availableFieldList (list of strings; default PropTypes.arrayOf(PropTypes.string))

- cellUpdate (dict; default PropTypes.object):
    Deprecated compatibility input. Editing is unsupported and this
    value is ignored.

- cellUpdates (list of dicts; default PropTypes.arrayOf(PropTypes.object)):
    Deprecated compatibility input. Editing is unsupported and this
    value is ignored.

- chartCanvasPanes (list of dicts; default PropTypes.arrayOf(PropTypes.shape({    id: PropTypes.string,    name: PropTypes.string,    chartTitle: PropTypes.string,    source: PropTypes.oneOf(['pivot', 'selection']),    chartType: PropTypes.string,    barLayout: PropTypes.oneOf(['grouped', 'stacked']),    axisMode: PropTypes.oneOf(['vertical', 'horizontal']),    orientation: PropTypes.oneOf(['rows', 'columns']),    hierarchyLevel: PropTypes.oneOfType([PropTypes.number, PropTypes.oneOf(['all'])]),    rowLimit: PropTypes.number,    columnLimit: PropTypes.number,    width: PropTypes.number,    chartHeight: PropTypes.number,    sortMode: PropTypes.string,    interactionMode: PropTypes.string,    serverScope: PropTypes.oneOf(['viewport', 'root']),    size: PropTypes.number,    floating: PropTypes.bool,    settingsPanelWidth: PropTypes.number,    chartSettings: PropTypes.shape({      chartSubtitle: PropTypes.string,      yAxisTitle: PropTypes.string,      yAxisMin: PropTypes.number,      yAxisMax: PropTypes.number,      yAxisMinRight: PropTypes.number,      yAxisMaxRight: PropTypes.number,      tickCount: PropTypes.number,      labelAngle: PropTypes.number,      colorPalette: PropTypes.string,      valueFormat: PropTypes.string,      showDataLabels: PropTypes.bool,      decimalPlaces: PropTypes.number,      labelPosition: PropTypes.string,      legendPosition: PropTypes.string,      markerShape: PropTypes.string,      markerSize: PropTypes.number,      showGradient: PropTypes.bool,      gradientDirection: PropTypes.string,      gradientOpacity: PropTypes.number,      showAnimations: PropTypes.bool,      showCalloutLabels: PropTypes.bool,      showTrendline: PropTypes.bool,      trendlineMode: PropTypes.string,      trendlinePeriod: PropTypes.number,      trendlineColor: PropTypes.string,      trendlineWidth: PropTypes.number,      seriesSort: PropTypes.string,      seriesColors: PropTypes.object,      customPaletteColors: PropTypes.arrayOf(PropTypes.string),      lineDashStyles: PropTypes.object,      referenceLines: PropTypes.arrayOf(PropTypes.object)    })  })))

    `chartCanvasPanes` is a list of dicts with keys:

    - id (string; optional)

    - name (string; optional)

    - chartTitle (string; optional)

    - source (string; optional)

    - chartType (string; optional)

    - barLayout (string; optional)

    - axisMode (string; optional)

    - orientation (string; optional)

    - hierarchyLevel (string | number; optional)

    - rowLimit (number; optional)

    - columnLimit (number; optional)

    - width (number; optional)

    - chartHeight (number; optional)

    - sortMode (string; optional)

    - interactionMode (string; optional)

    - serverScope (string; optional)

    - size (number; optional)

    - floating (dict; optional)

        `floating` is a dict with keys:

        - valueOf (optional):

            Returns the primitive value of the specified object.

              Or dict with keys:

        - valueOf (optional):

            Returns the primitive value of the specified object.

    - settingsPanelWidth (number; optional)

    - chartSettings (dict; optional)

        `chartSettings` is a dict with keys:

        - chartSubtitle (string; optional)

        - yAxisTitle (string; optional)

        - yAxisMin (number; optional)

        - yAxisMax (number; optional)

        - yAxisMinRight (number; optional)

        - yAxisMaxRight (number; optional)

        - tickCount (number; optional)

        - labelAngle (number; optional)

        - colorPalette (string; optional)

        - valueFormat (string; optional)

        - showDataLabels (dict; optional)

            `showDataLabels` is a dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

                  Or dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

        - decimalPlaces (number; optional)

        - labelPosition (string; optional)

        - legendPosition (string; optional)

        - markerShape (string; optional)

        - markerSize (number; optional)

        - showGradient (dict; optional)

            `showGradient` is a dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

                  Or dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

        - gradientDirection (string; optional)

        - gradientOpacity (number; optional)

        - showAnimations (dict; optional)

            `showAnimations` is a dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

                  Or dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

        - showCalloutLabels (dict; optional)

            `showCalloutLabels` is a dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

                  Or dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

        - showTrendline (dict; optional)

            `showTrendline` is a dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

                  Or dict with keys:

            - valueOf (optional):

                Returns the primitive value of the specified object.

        - trendlineMode (string; optional)

        - trendlinePeriod (number; optional)

        - trendlineColor (string; optional)

        - trendlineWidth (number; optional)

        - seriesSort (string; optional)

        - seriesColors (dict; optional)

        - customPaletteColors (list of strings; optional)

        - lineDashStyles (dict; optional)

        - referenceLines (list of dicts; optional)

- chartDefaults (dict; default PropTypes.object)

- chartDefinitions (list of dicts; default PropTypes.arrayOf(PropTypes.object))

- chartEvent (dict; default PropTypes.object)

- chartServerWindow (dict; default PropTypes.shape({    enabled: PropTypes.bool,    rows: PropTypes.number,    columns: PropTypes.number,    scope: PropTypes.oneOf(['viewport', 'root'])  }))

- colFields (list of dicts; default PropTypes.array)

    `colFields` is a list of dicts with keys:


- columnOrder (list of strings; default PropTypes.arrayOf(PropTypes.string))

- columnPinned (dict; default PropTypes.object)

- columnPinning (dict; default PropTypes.shape({    left: PropTypes.arrayOf(PropTypes.string),    right: PropTypes.arrayOf(PropTypes.string)  }))

- columnSizing (dict; default PropTypes.object)

- columnVisibility (dict; default PropTypes.object)

- conditionalFormatting (list of dicts; default PropTypes.arrayOf(PropTypes.object))

- customDimensions (list of dicts; default PropTypes.arrayOf(PropTypes.object))

- data (list of dicts; default PropTypes.arrayOf(PropTypes.object))

- decimalPlaces (number; default PropTypes.number)

- defaultTheme (string; default PropTypes.string)

- defaultValueFormat (string; default PropTypes.string)

- detailConfig (dict; default PropTypes.object)

- detailMode (string; default PropTypes.oneOf(['none', 'inline', 'sidepanel', 'drawer']))

- drillEndpoint (string; default PropTypes.string)

- duckdbBundleUrl (string; default PropTypes.string)

- editLifecycleEvent (dict; default PropTypes.object):
    Deprecated compatibility input. Editing is unsupported and this
    value is ignored.

- editState (dict; default PropTypes.object):
    Deprecated compatibility input. Editing is unsupported and this
    value is ignored.

- editingConfig (dict; default PropTypes.object):
    Deprecated compatibility input. Editing is unsupported and this
    value is ignored.

- expanded (dict; default PropTypes.oneOfType([PropTypes.object, PropTypes.bool]))

    `expanded` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object. | dict

- fieldPanelSizes (dict; default PropTypes.object)

- filters (dict; default PropTypes.object)

- fitToWidth (dict; default PropTypes.bool)

    `fitToWidth` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- grandTotalPosition (string; default PropTypes.oneOf(['top', 'bottom']))

- groupAllowUnbalanced (dict; default PropTypes.bool)

    `groupAllowUnbalanced` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- headerFormatting (dict | list of dicts; default PropTypes.oneOfType([PropTypes.object, PropTypes.arrayOf(PropTypes.object)]))

- immersiveExitPosition (string; default PropTypes.oneOf(['top-right', 'bottom-right', 'hidden']))

- immersiveMode (dict; default PropTypes.bool)

    `immersiveMode` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- loadWebFonts (dict; default PropTypes.bool)

    `loadWebFonts` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- localeText (dict; default PropTypes.object)

- measureAxis (dict; default PropTypes.shape({    placement: PropTypes.oneOf(['none', 'rows', 'columns']),    labelField: PropTypes.string,    valueField: PropTypes.string,    members: PropTypes.arrayOf(PropTypes.object),    suppressEmptyMembers: PropTypes.bool,    suppressZeroMembers: PropTypes.bool,    totalsPolicy: PropTypes.string  }))

- measureGroups (list of dicts; default PropTypes.arrayOf(PropTypes.object))

- mergedHeaderSeparator (string; default PropTypes.string)

- numberGroupSeparator (string; default PropTypes.oneOf(['comma', 'space', 'thin_space', 'apostrophe', 'none']))

- paginationConfig (dict; default PropTypes.object)

- performanceConfig (dict; default PropTypes.shape({    cacheBlockSize: PropTypes.number,    maxBlocksInCache: PropTypes.number,    blockLoadDebounceMs: PropTypes.number,    rowOverscan: PropTypes.number,    columnOverscan: PropTypes.number,    prefetchColumns: PropTypes.number,    executionMode: PropTypes.oneOf(['auto', 'client', 'server']),    clientEngine: PropTypes.oneOf(['auto', 'duckdb-wasm', 'memory']),    duckdbWasmThreshold: PropTypes.number,    clientRowThreshold: PropTypes.number,    dataUrl: PropTypes.string,    duckdbWasmCdnUrl: PropTypes.string,    legacyClientTable: PropTypes.bool  }))

- persistence (dict; default PropTypes.oneOfType([PropTypes.bool, PropTypes.string, PropTypes.number]))

    `persistence` is a string | number | dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- persistence_type (string; default PropTypes.oneOf(['local', 'session', 'memory']))

- pinningOptions (dict; default PropTypes.shape({    maxPinnedLeft: PropTypes.number,    maxPinnedRight: PropTypes.number,    suppressMovable: PropTypes.bool,    lockPinned: PropTypes.bool  }))

- pinningPresets (list of dicts; default PropTypes.arrayOf(PropTypes.shape({    name: PropTypes.string,    config: PropTypes.object  })))

- pivotTitle (string; default PropTypes.string)

- reportDef (dict; default PropTypes.object)

- reset (boolean | number | string | dict | list; default PropTypes.any)

- rowFields (list of dicts; default PropTypes.array)

    `rowFields` is a list of dicts with keys:


- rowHeaderLabel (string; default PropTypes.string)

- rowMove (dict; default PropTypes.object)

- rowPinned (dict; default PropTypes.object)

- rowPinning (dict; default PropTypes.shape({    top: PropTypes.arrayOf(PropTypes.string),    bottom: PropTypes.arrayOf(PropTypes.string)  }))

- rowSelection (dict; default PropTypes.oneOfType([PropTypes.oneOf(['single', 'multiple']), PropTypes.bool]))

    `rowSelection` is a string | dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- runtimeRequest (dict; default PropTypes.object)

- runtimeResponse (dict; default PropTypes.object)

- saveViewTrigger (boolean | number | string | dict | list; default PropTypes.any)

- savedReports (list of dicts; default PropTypes.arrayOf(PropTypes.object))

- savedView (dict; default PropTypes.object)

- selectedRows (list of strings; default PropTypes.arrayOf(PropTypes.string))

- serverSide (dict; default PropTypes.bool)

    `serverSide` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- setProps (default PropTypes.func)

- showColTotals (dict; default PropTypes.bool)

    `showColTotals` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- showGrandTotal (dict; default PropTypes.bool)

    `showGrandTotal` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- showRowTotals (dict; default PropTypes.bool)

    `showRowTotals` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- showSubtotals (dict; default PropTypes.bool)

    `showSubtotals` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- sortEvent (dict; default PropTypes.object)

- sortLock (dict; default PropTypes.bool)

    `sortLock` is a dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

      Or dict with keys:

    - valueOf (optional):
        Returns the primitive value of the specified object.

- sortOptions (dict; default PropTypes.shape({    naturalSort: PropTypes.bool,    caseSensitive: PropTypes.bool,    columnOptions: PropTypes.object  }))

- sorting (list of dicts; default PropTypes.array)

    `sorting` is a list of dicts with keys:


- sparklineFields (dict; default PropTypes.object)

- table (string; default PropTypes.string)

- tableCanvasSize (number; default PropTypes.number)

- themeOverrides (dict; default PropTypes.object)

- treeConfig (dict; default PropTypes.object)

- uiConfig (dict; default PropTypes.object)

- unbalancedValues (list of dicts; default PropTypes.array)

    `unbalancedValues` is a list of dicts with keys:


- unitSuffix (string; default PropTypes.string)

- valConfigs (list of dicts; default PropTypes.arrayOf(PropTypes.shape({    id: PropTypes.string,    field: PropTypes.string,    agg: PropTypes.oneOf(['sum', 'avg', 'count', 'min', 'max', 'weighted_avg', 'wavg', 'weighted_mean', 'array_agg', 'formula']),    weightField: PropTypes.string,    windowFn: PropTypes.oneOf(['percent_of_row', 'percent_of_col', 'percent_of_grand_total']),    format: PropTypes.string,    percentile: PropTypes.number,    separator: PropTypes.string,    formula: PropTypes.string,    label: PropTypes.string,    shortLabel: PropTypes.string,    headerGroup: PropTypes.string,    valueScale: PropTypes.number,    unitSuffix: PropTypes.string,    formulaRef: PropTypes.string,    formulaScope: PropTypes.oneOf(['measures', 'columns']),    headerStyle: PropTypes.object,    headerBg: PropTypes.string,    headerColor: PropTypes.string,    headerTextColor: PropTypes.string,    sparkline: PropTypes.oneOfType([PropTypes.bool, PropTypes.object])  })))

    `valConfigs` is a list of dicts with keys:

    - id (string; optional)

    - field (string; optional)

    - agg (string; optional)

    - weightField (string; optional)

    - windowFn (string; optional)

    - format (string; optional)

    - percentile (number; optional)

    - separator (string; optional)

    - formula (string; optional)

    - label (string; optional)

    - shortLabel (string; optional)

    - headerGroup (string; optional)

    - valueScale (number; optional)

    - unitSuffix (string; optional)

    - formulaRef (string; optional)

    - formulaScope (string; optional)

    - headerStyle (dict; optional)

    - headerBg (string; optional)

    - headerColor (string; optional)

    - headerTextColor (string; optional)

    - sparkline (dict; optional)

        `sparkline` is a dict with keys:

        - valueOf (optional):

            Returns the primitive value of the specified object.

              Or dict with keys:

        - valueOf (optional):

            Returns the primitive value of the specified object. | dict

- validationRules (dict; default PropTypes.object)

- valueScale (number; default PropTypes.number)

- viewMode (string; default PropTypes.oneOf(['pivot', 'report', 'tree', 'table']))

- viewState (dict; default PropTypes.object)"""
    _children_props = []
    _base_nodes = ['children']
    _namespace = 'serverside_pivot'
    _type = 'DashTanstackPivot'
    ServerSide = TypedDict(
        "ServerSide",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    RowFields = TypedDict(
        "RowFields",
            {

        }
    )

    ColFields = TypedDict(
        "ColFields",
            {

        }
    )

    ValConfigsSparkline = TypedDict(
        "ValConfigsSparkline",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ValConfigs = TypedDict(
        "ValConfigs",
            {
            "id": NotRequired[typing.Union[str]],
            "field": NotRequired[typing.Union[str]],
            "agg": NotRequired[typing.Union[str]],
            "weightField": NotRequired[typing.Union[str]],
            "windowFn": NotRequired[typing.Union[str]],
            "format": NotRequired[typing.Union[str]],
            "percentile": NotRequired[typing.Union[NumberType]],
            "separator": NotRequired[typing.Union[str]],
            "formula": NotRequired[typing.Union[str]],
            "label": NotRequired[typing.Union[str]],
            "shortLabel": NotRequired[typing.Union[str]],
            "headerGroup": NotRequired[typing.Union[str]],
            "valueScale": NotRequired[typing.Union[NumberType]],
            "unitSuffix": NotRequired[typing.Union[str]],
            "formulaRef": NotRequired[typing.Union[str]],
            "formulaScope": NotRequired[typing.Union[str]],
            "headerStyle": NotRequired[typing.Union[dict]],
            "headerBg": NotRequired[typing.Union[str]],
            "headerColor": NotRequired[typing.Union[str]],
            "headerTextColor": NotRequired[typing.Union[str]],
            "sparkline": NotRequired[typing.Union["ValConfigsSparkline", dict]]
        }
    )

    Sorting = TypedDict(
        "Sorting",
            {

        }
    )

    Expanded = TypedDict(
        "Expanded",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ImmersiveMode = TypedDict(
        "ImmersiveMode",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ShowRowTotals = TypedDict(
        "ShowRowTotals",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ShowColTotals = TypedDict(
        "ShowColTotals",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ShowSubtotals = TypedDict(
        "ShowSubtotals",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ChartCanvasPanesFloating = TypedDict(
        "ChartCanvasPanesFloating",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ChartCanvasPanesChartSettingsShowDataLabels = TypedDict(
        "ChartCanvasPanesChartSettingsShowDataLabels",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ChartCanvasPanesChartSettingsShowGradient = TypedDict(
        "ChartCanvasPanesChartSettingsShowGradient",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ChartCanvasPanesChartSettingsShowAnimations = TypedDict(
        "ChartCanvasPanesChartSettingsShowAnimations",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ChartCanvasPanesChartSettingsShowCalloutLabels = TypedDict(
        "ChartCanvasPanesChartSettingsShowCalloutLabels",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ChartCanvasPanesChartSettingsShowTrendline = TypedDict(
        "ChartCanvasPanesChartSettingsShowTrendline",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    ChartCanvasPanesChartSettings = TypedDict(
        "ChartCanvasPanesChartSettings",
            {
            "chartSubtitle": NotRequired[typing.Union[str]],
            "yAxisTitle": NotRequired[typing.Union[str]],
            "yAxisMin": NotRequired[typing.Union[NumberType]],
            "yAxisMax": NotRequired[typing.Union[NumberType]],
            "yAxisMinRight": NotRequired[typing.Union[NumberType]],
            "yAxisMaxRight": NotRequired[typing.Union[NumberType]],
            "tickCount": NotRequired[typing.Union[NumberType]],
            "labelAngle": NotRequired[typing.Union[NumberType]],
            "colorPalette": NotRequired[typing.Union[str]],
            "valueFormat": NotRequired[typing.Union[str]],
            "showDataLabels": NotRequired[typing.Union["ChartCanvasPanesChartSettingsShowDataLabels"]],
            "decimalPlaces": NotRequired[typing.Union[NumberType]],
            "labelPosition": NotRequired[typing.Union[str]],
            "legendPosition": NotRequired[typing.Union[str]],
            "markerShape": NotRequired[typing.Union[str]],
            "markerSize": NotRequired[typing.Union[NumberType]],
            "showGradient": NotRequired[typing.Union["ChartCanvasPanesChartSettingsShowGradient"]],
            "gradientDirection": NotRequired[typing.Union[str]],
            "gradientOpacity": NotRequired[typing.Union[NumberType]],
            "showAnimations": NotRequired[typing.Union["ChartCanvasPanesChartSettingsShowAnimations"]],
            "showCalloutLabels": NotRequired[typing.Union["ChartCanvasPanesChartSettingsShowCalloutLabels"]],
            "showTrendline": NotRequired[typing.Union["ChartCanvasPanesChartSettingsShowTrendline"]],
            "trendlineMode": NotRequired[typing.Union[str]],
            "trendlinePeriod": NotRequired[typing.Union[NumberType]],
            "trendlineColor": NotRequired[typing.Union[str]],
            "trendlineWidth": NotRequired[typing.Union[NumberType]],
            "seriesSort": NotRequired[typing.Union[str]],
            "seriesColors": NotRequired[typing.Union[dict]],
            "customPaletteColors": NotRequired[typing.Union[typing.Sequence[str]]],
            "lineDashStyles": NotRequired[typing.Union[dict]],
            "referenceLines": NotRequired[typing.Union[typing.Sequence[dict]]]
        }
    )

    ChartCanvasPanes = TypedDict(
        "ChartCanvasPanes",
            {
            "id": NotRequired[typing.Union[str]],
            "name": NotRequired[typing.Union[str]],
            "chartTitle": NotRequired[typing.Union[str]],
            "source": NotRequired[typing.Union[str]],
            "chartType": NotRequired[typing.Union[str]],
            "barLayout": NotRequired[typing.Union[str]],
            "axisMode": NotRequired[typing.Union[str]],
            "orientation": NotRequired[typing.Union[str]],
            "hierarchyLevel": NotRequired[typing.Union[str, NumberType]],
            "rowLimit": NotRequired[typing.Union[NumberType]],
            "columnLimit": NotRequired[typing.Union[NumberType]],
            "width": NotRequired[typing.Union[NumberType]],
            "chartHeight": NotRequired[typing.Union[NumberType]],
            "sortMode": NotRequired[typing.Union[str]],
            "interactionMode": NotRequired[typing.Union[str]],
            "serverScope": NotRequired[typing.Union[str]],
            "size": NotRequired[typing.Union[NumberType]],
            "floating": NotRequired[typing.Union["ChartCanvasPanesFloating"]],
            "settingsPanelWidth": NotRequired[typing.Union[NumberType]],
            "chartSettings": NotRequired[typing.Union["ChartCanvasPanesChartSettings"]]
        }
    )

    Persistence = TypedDict(
        "Persistence",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    SortLock = TypedDict(
        "SortLock",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    LoadWebFonts = TypedDict(
        "LoadWebFonts",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    GroupAllowUnbalanced = TypedDict(
        "GroupAllowUnbalanced",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    UnbalancedValues = TypedDict(
        "UnbalancedValues",
            {

        }
    )

    ShowGrandTotal = TypedDict(
        "ShowGrandTotal",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    FitToWidth = TypedDict(
        "FitToWidth",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )

    RowSelection = TypedDict(
        "RowSelection",
            {
            "valueOf": NotRequired[typing.Any]
        }
    )


    def __init__(
        self,
        id: typing.Optional[typing.Union[str, dict]] = None,
        table: typing.Optional[typing.Union[str]] = None,
        pivotTitle: typing.Optional[typing.Union[str]] = None,
        data: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        style: typing.Optional[typing.Any] = None,
        serverSide: typing.Optional[typing.Union["ServerSide"]] = None,
        rowFields: typing.Optional[typing.Union[typing.Sequence["RowFields"]]] = None,
        colFields: typing.Optional[typing.Union[typing.Sequence["ColFields"]]] = None,
        valConfigs: typing.Optional[typing.Union[typing.Sequence["ValConfigs"]]] = None,
        measureAxis: typing.Optional[typing.Union[dict]] = None,
        filters: typing.Optional[typing.Union[dict]] = None,
        sorting: typing.Optional[typing.Union[typing.Sequence["Sorting"]]] = None,
        expanded: typing.Optional[typing.Union["Expanded", dict]] = None,
        immersiveMode: typing.Optional[typing.Union["ImmersiveMode"]] = None,
        showRowTotals: typing.Optional[typing.Union["ShowRowTotals"]] = None,
        showColTotals: typing.Optional[typing.Union["ShowColTotals"]] = None,
        showSubtotals: typing.Optional[typing.Union["ShowSubtotals"]] = None,
        grandTotalPosition: typing.Optional[typing.Union[str]] = None,
        runtimeRequest: typing.Optional[typing.Union[dict]] = None,
        runtimeResponse: typing.Optional[typing.Union[dict]] = None,
        viewMode: typing.Optional[typing.Union[str]] = None,
        detailMode: typing.Optional[typing.Union[str]] = None,
        treeConfig: typing.Optional[typing.Union[dict]] = None,
        detailConfig: typing.Optional[typing.Union[dict]] = None,
        chartEvent: typing.Optional[typing.Union[dict]] = None,
        chartDefinitions: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        chartDefaults: typing.Optional[typing.Union[dict]] = None,
        chartCanvasPanes: typing.Optional[typing.Union[typing.Sequence["ChartCanvasPanes"]]] = None,
        tableCanvasSize: typing.Optional[typing.Union[NumberType]] = None,
        performanceConfig: typing.Optional[typing.Union[dict]] = None,
        chartServerWindow: typing.Optional[typing.Union[dict]] = None,
        cellUpdate: typing.Optional[typing.Union[dict]] = None,
        cellUpdates: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        rowMove: typing.Optional[typing.Union[dict]] = None,
        drillEndpoint: typing.Optional[typing.Union[str]] = None,
        viewState: typing.Optional[typing.Union[dict]] = None,
        saveViewTrigger: typing.Optional[typing.Any] = None,
        savedView: typing.Optional[typing.Union[dict]] = None,
        conditionalFormatting: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        headerFormatting: typing.Optional[typing.Union[dict, typing.Sequence[dict]]] = None,
        validationRules: typing.Optional[typing.Union[dict]] = None,
        editingConfig: typing.Optional[typing.Union[dict]] = None,
        editLifecycleEvent: typing.Optional[typing.Union[dict]] = None,
        editState: typing.Optional[typing.Union[dict]] = None,
        uiConfig: typing.Optional[typing.Union[dict]] = None,
        paginationConfig: typing.Optional[typing.Union[dict]] = None,
        mergedHeaderSeparator: typing.Optional[typing.Union[str]] = None,
        columnPinning: typing.Optional[typing.Union[dict]] = None,
        rowPinning: typing.Optional[typing.Union[dict]] = None,
        columnPinned: typing.Optional[typing.Union[dict]] = None,
        rowPinned: typing.Optional[typing.Union[dict]] = None,
        columnVisibility: typing.Optional[typing.Union[dict]] = None,
        columnOrder: typing.Optional[typing.Union[typing.Sequence[str]]] = None,
        columnSizing: typing.Optional[typing.Union[dict]] = None,
        localeText: typing.Optional[typing.Union[dict]] = None,
        decimalPlaces: typing.Optional[typing.Union[NumberType]] = None,
        defaultValueFormat: typing.Optional[typing.Union[str]] = None,
        fieldPanelSizes: typing.Optional[typing.Union[dict]] = None,
        numberGroupSeparator: typing.Optional[typing.Union[str]] = None,
        reset: typing.Optional[typing.Any] = None,
        persistence: typing.Optional[typing.Union[str, NumberType, "Persistence"]] = None,
        persistence_type: typing.Optional[typing.Union[str]] = None,
        pinningOptions: typing.Optional[typing.Union[dict]] = None,
        pinningPresets: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        sortOptions: typing.Optional[typing.Union[dict]] = None,
        sortLock: typing.Optional[typing.Union["SortLock"]] = None,
        defaultTheme: typing.Optional[typing.Union[str]] = None,
        themeOverrides: typing.Optional[typing.Union[dict]] = None,
        loadWebFonts: typing.Optional[typing.Union["LoadWebFonts"]] = None,
        immersiveExitPosition: typing.Optional[typing.Union[str]] = None,
        groupAllowUnbalanced: typing.Optional[typing.Union["GroupAllowUnbalanced"]] = None,
        unbalancedValues: typing.Optional[typing.Union[typing.Sequence["UnbalancedValues"]]] = None,
        showGrandTotal: typing.Optional[typing.Union["ShowGrandTotal"]] = None,
        measureGroups: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        rowHeaderLabel: typing.Optional[typing.Union[str]] = None,
        valueScale: typing.Optional[typing.Union[NumberType]] = None,
        unitSuffix: typing.Optional[typing.Union[str]] = None,
        fitToWidth: typing.Optional[typing.Union["FitToWidth"]] = None,
        autoSizeStrategy: typing.Optional[typing.Union[str]] = None,
        duckdbBundleUrl: typing.Optional[typing.Union[str]] = None,
        sortEvent: typing.Optional[typing.Union[dict]] = None,
        availableFieldList: typing.Optional[typing.Union[typing.Sequence[str]]] = None,
        sparklineFields: typing.Optional[typing.Union[dict]] = None,
        customDimensions: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        reportDef: typing.Optional[typing.Union[dict]] = None,
        savedReports: typing.Optional[typing.Union[typing.Sequence[dict]]] = None,
        activeReportId: typing.Optional[typing.Union[str]] = None,
        rowSelection: typing.Optional[typing.Union[str, "RowSelection"]] = None,
        selectedRows: typing.Optional[typing.Union[typing.Sequence[str]]] = None,
        **kwargs
    ):
        self._prop_names = ['id', 'activeReportId', 'autoSizeStrategy', 'availableFieldList', 'cellUpdate', 'cellUpdates', 'chartCanvasPanes', 'chartDefaults', 'chartDefinitions', 'chartEvent', 'chartServerWindow', 'colFields', 'columnOrder', 'columnPinned', 'columnPinning', 'columnSizing', 'columnVisibility', 'conditionalFormatting', 'customDimensions', 'data', 'decimalPlaces', 'defaultTheme', 'defaultValueFormat', 'detailConfig', 'detailMode', 'drillEndpoint', 'duckdbBundleUrl', 'editLifecycleEvent', 'editState', 'editingConfig', 'expanded', 'fieldPanelSizes', 'filters', 'fitToWidth', 'grandTotalPosition', 'groupAllowUnbalanced', 'headerFormatting', 'immersiveExitPosition', 'immersiveMode', 'loadWebFonts', 'localeText', 'measureAxis', 'measureGroups', 'mergedHeaderSeparator', 'numberGroupSeparator', 'paginationConfig', 'performanceConfig', 'persistence', 'persistence_type', 'pinningOptions', 'pinningPresets', 'pivotTitle', 'reportDef', 'reset', 'rowFields', 'rowHeaderLabel', 'rowMove', 'rowPinned', 'rowPinning', 'rowSelection', 'runtimeRequest', 'runtimeResponse', 'saveViewTrigger', 'savedReports', 'savedView', 'selectedRows', 'serverSide', 'setProps', 'showColTotals', 'showGrandTotal', 'showRowTotals', 'showSubtotals', 'sortEvent', 'sortLock', 'sortOptions', 'sorting', 'sparklineFields', 'style', 'table', 'tableCanvasSize', 'themeOverrides', 'treeConfig', 'uiConfig', 'unbalancedValues', 'unitSuffix', 'valConfigs', 'validationRules', 'valueScale', 'viewMode', 'viewState']
        self._valid_wildcard_attributes =            []
        self.available_properties = ['id', 'activeReportId', 'autoSizeStrategy', 'availableFieldList', 'cellUpdate', 'cellUpdates', 'chartCanvasPanes', 'chartDefaults', 'chartDefinitions', 'chartEvent', 'chartServerWindow', 'colFields', 'columnOrder', 'columnPinned', 'columnPinning', 'columnSizing', 'columnVisibility', 'conditionalFormatting', 'customDimensions', 'data', 'decimalPlaces', 'defaultTheme', 'defaultValueFormat', 'detailConfig', 'detailMode', 'drillEndpoint', 'duckdbBundleUrl', 'editLifecycleEvent', 'editState', 'editingConfig', 'expanded', 'fieldPanelSizes', 'filters', 'fitToWidth', 'grandTotalPosition', 'groupAllowUnbalanced', 'headerFormatting', 'immersiveExitPosition', 'immersiveMode', 'loadWebFonts', 'localeText', 'measureAxis', 'measureGroups', 'mergedHeaderSeparator', 'numberGroupSeparator', 'paginationConfig', 'performanceConfig', 'persistence', 'persistence_type', 'pinningOptions', 'pinningPresets', 'pivotTitle', 'reportDef', 'reset', 'rowFields', 'rowHeaderLabel', 'rowMove', 'rowPinned', 'rowPinning', 'rowSelection', 'runtimeRequest', 'runtimeResponse', 'saveViewTrigger', 'savedReports', 'savedView', 'selectedRows', 'serverSide', 'setProps', 'showColTotals', 'showGrandTotal', 'showRowTotals', 'showSubtotals', 'sortEvent', 'sortLock', 'sortOptions', 'sorting', 'sparklineFields', 'style', 'table', 'tableCanvasSize', 'themeOverrides', 'treeConfig', 'uiConfig', 'unbalancedValues', 'unitSuffix', 'valConfigs', 'validationRules', 'valueScale', 'viewMode', 'viewState']
        self.available_wildcard_properties =            []
        _explicit_args = kwargs.pop('_explicit_args')
        _locals = locals()
        _locals.update(kwargs)  # For wildcard attrs and excess named props
        args = {k: _locals[k] for k in _explicit_args}

        super(DashTanstackPivot, self).__init__(**args)

setattr(DashTanstackPivot, "__init__", _explicitize_args(DashTanstackPivot.__init__))
