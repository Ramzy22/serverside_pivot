import { useState } from 'react';

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
    const [colorScaleMode, setColorScaleMode] = useState('off');
    const [colorPalette, setColorPalette] = useState('redGreen');
    const [dataBarsColumns, setDataBarsColumns] = useState(new Set());

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
        colorPalette, setColorPalette,
        dataBarsColumns, setDataBarsColumns,
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
