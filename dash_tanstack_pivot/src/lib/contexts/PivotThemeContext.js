import React, { createContext, useContext, useEffect, useMemo } from 'react';
import { injectThemeCssVars } from '../utils/styles';

const PivotThemeContext = createContext(null);

function injectCssBaseline() {
    if (typeof document === 'undefined' || document.getElementById('sp-css-baseline')) return;
    const style = document.createElement('style');
    style.id = 'sp-css-baseline';
    style.textContent = `:root{--sp-font-sans:'Inter',ui-sans-serif,system-ui,sans-serif;--sp-font-mono:'JetBrains Mono','Courier New',monospace;}`;
    document.head.appendChild(style);
}

export function PivotThemeProvider({ theme, styles, children }) {
    useEffect(() => { injectCssBaseline(); }, []);
    useEffect(() => { injectThemeCssVars(theme); }, [theme]);
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
