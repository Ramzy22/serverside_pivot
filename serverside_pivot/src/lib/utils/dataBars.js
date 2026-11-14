/** Saved-view overrides are positive magnitudes; an absent entry means Auto.  @param {any} [value] @returns {any} */
export const normalizeDataBarMaximums = (value) => Object.fromEntries(
    Object.entries(value && typeof value === 'object' && !Array.isArray(value) ? value : {})
        .filter(([column, maximum]) => column && typeof maximum === 'number' && Number.isFinite(maximum) && maximum > 0)
);

/** @param {any} [previous] @param {any} [columns] @param {any} [maximum] @returns {any} */
export const updateDataBarMaximums = (previous, columns, maximum) => {
    /** @type {any} */
    const next = normalizeDataBarMaximums(previous);
    columns.forEach(/** @param {any} [column] @returns {any} */ column => {
        if (typeof maximum === 'number' && Number.isFinite(maximum) && maximum > 0) next[column] = maximum;
        else delete next[column];
    });
    return next;
};

/** @param {any} [maximum] @param {any} [stats] @returns {any} */
export const resolveDataBarMaximum = (maximum, stats) => {
    if (typeof maximum === 'number' && Number.isFinite(maximum) && maximum > 0) return maximum;
    return stats && Number.isFinite(stats.min) && Number.isFinite(stats.max)
        ? Math.max(Math.abs(stats.max), Math.abs(stats.min), 1e-9) : null;
};
