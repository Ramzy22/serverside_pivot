// Reports own their hierarchy plus the value/column axes used to render it.
// Optional layout keeps previously persisted report records readable.
/** @param {any} [colFields] @param {any} [valConfigs] @returns {any} */
export function captureReportLayout(colFields, valConfigs) {
    return JSON.parse(JSON.stringify({
        colFields: Array.isArray(colFields) ? colFields : [],
        valConfigs: Array.isArray(valConfigs) ? valConfigs : [],
    }));
}

/** @param {any} report @param {any} p1 @returns {any} */
export function applySavedReport(report, { setReportDef, setActiveReportId, setColFields, setValConfigs }) {
    if (!report || !report.reportDef || typeof setReportDef !== 'function') return false;
    const layout = report.layout;
    if (layout && Array.isArray(layout.colFields) && Array.isArray(layout.valConfigs)) {
        if (typeof setColFields === 'function') setColFields([...layout.colFields]);
        if (typeof setValConfigs === 'function') setValConfigs(JSON.parse(JSON.stringify(layout.valConfigs)));
    } else if (typeof setColFields === 'function') {
        // Legacy reports did not retain pivot columns and rendered flat values.
        setColFields([]);
    }
    setReportDef(JSON.parse(JSON.stringify(report.reportDef)));
    if (typeof setActiveReportId === 'function') setActiveReportId(report.id);
    return true;
}
