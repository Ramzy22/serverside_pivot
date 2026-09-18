/** Pure layout and localized-text helpers used by the pivot composition layer. */

export const getStickyHeaderHeight = (headerGroupCount, rowHeight, showFloatingFilters) => (
    (headerGroupCount * rowHeight) + (showFloatingFilters ? rowHeight : 0)
);

export const formatLocaleTextTemplate = (template, values = {}) => {
    const text = template === null || template === undefined ? '' : String(template);
    if (!values || typeof values !== 'object') return text;
    return text.replace(/\{([A-Za-z0-9_]+)\}/g, (match, key) => (
        Object.prototype.hasOwnProperty.call(values, key) ? String(values[key]) : match
    ));
};
