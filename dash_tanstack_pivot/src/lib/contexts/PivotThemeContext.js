import React, { createContext, useContext, useMemo } from 'react';

const PivotThemeContext = createContext(null);

export function PivotThemeProvider({ theme, styles, children }) {
    const value = useMemo(() => ({ theme, styles }), [theme, styles]);
    return (
        <PivotThemeContext.Provider value={value}>
            {children}
        </PivotThemeContext.Provider>
    );
}

export function usePivotTheme() {
    const ctx = useContext(PivotThemeContext);
    if (!ctx) throw new Error('usePivotTheme must be used within PivotThemeProvider');
    return ctx;
}
