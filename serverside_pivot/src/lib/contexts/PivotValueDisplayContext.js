import React, { createContext, useContext, useMemo } from 'react';

const PivotValueDisplayContext = createContext(/** @type {any} */ (null));

/**
 * @param {Record<string, any>} props
 */
export function PivotValueDisplayProvider({
    editValueDisplayMode,
    resolveCellDisplayValue,
    resolveCurrentCellValue,
    setEditValueDisplayMode,
    hasComparedValues,
    children,
}) {
    const value = useMemo(() => ({
        editValueDisplayMode,
        resolveCellDisplayValue,
        resolveCurrentCellValue,
        setEditValueDisplayMode,
        hasComparedValues,
    }), [
        editValueDisplayMode,
        resolveCellDisplayValue,
        resolveCurrentCellValue,
        setEditValueDisplayMode,
        hasComparedValues,
    ]);

    return (
        <PivotValueDisplayContext.Provider value={value}>
            {children}
        </PivotValueDisplayContext.Provider>
    );
}

export function useOptionalPivotValueDisplay() {
    return useContext(PivotValueDisplayContext);
}
