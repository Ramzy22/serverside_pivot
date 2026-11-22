/**
 * One row identity for the whole frontend (REQ-ID-001).
 *
 * The canonical key is the typed path encoded by `CanonicalPathCodec`
 * (`_rowKey` from the core, `_canonicalPath` from the live engine; same
 * encoding, "[]" for the grand total). Display paths (`a|||b`) are ambiguous:
 * NULL and "" render alike and members may contain the separator, so they are
 * used only for rows that carry no canonical key (legacy responses).
 */

type RowLike = { [key: string]: unknown } | null | undefined;

export function canonicalRowKey(row: RowLike): string {
    if (!row) return '';
    const key = row._rowKey ?? row._canonicalPath;
    return typeof key === 'string' ? key : '';
}

/** The display path, or null when the row has none ("" is a real, ambiguous display path). */
export function displayRowKey(row: RowLike): string | null {
    if (!row) return null;
    const key = row._path ?? row._id;
    return key === null || key === undefined ? null : String(key);
}

/** The key to index and look up a row by: canonical when known, else its display path. */
export function rowIdentity(row: RowLike): string {
    return canonicalRowKey(row) || displayRowKey(row) || '';
}

/** Display keys shared by more than one canonical row in a frame (they cannot identify a row). */
export function ambiguousDisplayKeys(rows: RowLike[]): Set<string> {
    const owners = new Map<string, string>();
    const ambiguous = new Set<string>();
    for (const row of rows) {
        const display = displayRowKey(row);
        const canonical = canonicalRowKey(row);
        if (display === null || !canonical) continue;
        const owner = owners.get(display);
        if (owner === undefined) owners.set(display, canonical);
        else if (owner !== canonical) ambiguous.add(display);
    }
    return ambiguous;
}
