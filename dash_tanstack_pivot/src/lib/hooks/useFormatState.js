import { useCallback, useState } from 'react';
import { normalizeDataBarMaximums } from '../utils/dataBars';

/**
 * useFormatState — extracts formatting/display state from DashTanstackPivot.
 *
 * Covers: color scale, data bars, font, decimal, number format, cell format
 * rules, zoom, and hovered row path.
 */
export function useFormatState({
    normalizedInitialDecimalPlaces,
    normalizedInitialDefaultValueFormat,
    normalizedInitialNumberGroupSeparator,
}) {
    const [colorScaleMode, updateColorScaleMode] = useState('off');
    // null preserves the global scope of older saved views. A selection is
    // captured when the user applies a scale, not read from later selections.
    const [colorScaleColumns, setColorScaleColumns] = useState(null);
    const setColorScaleMode = useCallback((mode, columns = null) => {
        updateColorScaleMode(mode);
        setColorScaleColumns(Array.isArray(columns) && columns.length > 0
            ? [...new Set(columns.filter(column => typeof column === 'string'))] : null);
    }, []);
    const [colorPalette, setColorPalette] = useState('redGreen');
    const [dataBarsColumns, setDataBarsColumns] = useState(new Set());
    const [dataBarMaximums, updateDataBarMaximums] = useState({});
    const setDataBarMaximums = useCallback(value => {
        updateDataBarMaximums(previous => normalizeDataBarMaximums(
            typeof value === 'function' ? value(previous) : value
        ));
    }, []);

    // Font / display controls
    const [fontFamily, setFontFamily] = useState("'Inter', system-ui, sans-serif");
    const [fontSize, setFontSize] = useState('14px');
    const [decimalPlaces, setDecimalPlaces] = useState(normalizedInitialDecimalPlaces);
    const [defaultValueFormat, setDefaultValueFormat] = useState(normalizedInitialDefaultValueFormat);
    const [numberGroupSeparator, setNumberGroupSeparator] = useState(normalizedInitialNumberGroupSeparator);
    const [columnDecimalOverrides, setColumnDecimalOverrides] = useState({});
    const [columnFormatOverrides, setColumnFormatOverrides] = useState({});
    const [columnGroupSeparatorOverrides, setColumnGroupSeparatorOverrides] = useState({});
    const [cellFormatRules, setCellFormatRules] = useState({});
    const [hoveredRowPath, setHoveredRowPath] = useState(null);
    const [zoomLevel, setZoomLevel] = useState(100);

    return {
        colorScaleMode, setColorScaleMode,
        colorScaleColumns, setColorScaleColumns,
        colorPalette, setColorPalette,
        dataBarsColumns, setDataBarsColumns,
        dataBarMaximums, setDataBarMaximums,
        fontFamily, setFontFamily,
        fontSize, setFontSize,
        decimalPlaces, setDecimalPlaces,
        defaultValueFormat, setDefaultValueFormat,
        numberGroupSeparator, setNumberGroupSeparator,
        columnDecimalOverrides, setColumnDecimalOverrides,
        columnFormatOverrides, setColumnFormatOverrides,
        columnGroupSeparatorOverrides, setColumnGroupSeparatorOverrides,
        cellFormatRules, setCellFormatRules,
        hoveredRowPath, setHoveredRowPath,
        zoomLevel, setZoomLevel,
    };
}
