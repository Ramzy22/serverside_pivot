import { useCallback, useState } from 'react';
import { normalizeDataBarMaximums } from '../utils/dataBars';

/**
 * useFormatState — extracts formatting/display state from DashTanstackPivot.
 *
 * Covers: color scale, data bars, font, decimal, number format, cell format
 * rules, zoom, and hovered row path.
  @param {any} p0 @returns {any} */
export function useFormatState({
    normalizedInitialDecimalPlaces,
    normalizedInitialDefaultValueFormat,
    normalizedInitialNumberGroupSeparator,
}) {
    /** @type {[any, (...args: any[]) => any]} */
    const [colorScaleMode, updateColorScaleMode] = useState('off');
    // null preserves the global scope of older saved views. A selection is
    // captured when the user applies a scale, not read from later selections.
    /** @type {[any, (...args: any[]) => any]} */
    const [colorScaleColumns, setColorScaleColumns] = useState(null);
    /** @type {(...args: any[]) => any} */
    const setColorScaleMode = useCallback((mode, columns = null) => {
        updateColorScaleMode(mode);
        setColorScaleColumns(Array.isArray(columns) && columns.length > 0
            ? [...new Set(columns.filter(column => typeof column === 'string'))] : null);
    }, []);
    /** @type {[any, (...args: any[]) => any]} */
    const [colorPalette, setColorPalette] = useState('redGreen');
    /** @type {[any, (...args: any[]) => any]} */
    const [dataBarsColumns, setDataBarsColumns] = useState(new Set());
    /** @type {[any, (...args: any[]) => any]} */
    const [dataBarMaximums, updateDataBarMaximums] = useState({});
    /** @type {(...args: any[]) => any} */
    const setDataBarMaximums = useCallback(value => {
        updateDataBarMaximums(/** @param {any} [previous] @returns {any} */ previous => normalizeDataBarMaximums(
            typeof value === 'function' ? value(previous) : value
        ));
    }, []);

    // Font / display controls
    /** @type {[any, (...args: any[]) => any]} */
    const [fontFamily, setFontFamily] = useState("'Inter', system-ui, sans-serif");
    /** @type {[any, (...args: any[]) => any]} */
    const [fontSize, setFontSize] = useState('14px');
    /** @type {[any, (...args: any[]) => any]} */
    const [decimalPlaces, setDecimalPlaces] = useState(normalizedInitialDecimalPlaces);
    /** @type {[any, (...args: any[]) => any]} */
    const [defaultValueFormat, setDefaultValueFormat] = useState(normalizedInitialDefaultValueFormat);
    /** @type {[any, (...args: any[]) => any]} */
    const [numberGroupSeparator, setNumberGroupSeparator] = useState(normalizedInitialNumberGroupSeparator);
    /** @type {[any, (...args: any[]) => any]} */
    const [columnDecimalOverrides, setColumnDecimalOverrides] = useState({});
    /** @type {[any, (...args: any[]) => any]} */
    const [columnFormatOverrides, setColumnFormatOverrides] = useState({});
    /** @type {[any, (...args: any[]) => any]} */
    const [columnGroupSeparatorOverrides, setColumnGroupSeparatorOverrides] = useState({});
    /** @type {[any, (...args: any[]) => any]} */
    const [cellFormatRules, setCellFormatRules] = useState({});
    /** @type {[any, (...args: any[]) => any]} */
    const [hoveredRowPath, setHoveredRowPath] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
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
