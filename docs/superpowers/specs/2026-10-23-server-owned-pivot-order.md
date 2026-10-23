# Server-owned pivot column order

## Contract

The V1 runtime keeps one ordered pivot-column layout for viewport, copy, and export. A pivot leaf's saved identity comes from its pivot path and measure key. Labels, virtual indices, widths, and pinning are never identities. A row-total leaf has a distinct identity. The existing cell key remains the value lookup key, while a structured layout identity is carried in schema metadata.

Users may move whole pivot groups across the center area and reorder measure leaves within one group. Leaves cannot cross group boundaries. Row-total groups stay after ordinary pivot groups. The server validates or normalizes duplicate and unknown saved identities and never reveals unauthorized columns. A saved identity absent from the current authorized result stays in the saved order but occupies no visible slot; if it returns, it regains its saved relative position. New identities append in the server's deterministic default order.

The browser sends the saved order manifest on a change or cache miss. Ordinary viewport requests send a layout revision and a bounded list of pinned identities; they do not resend the full manifest. A worker may cache a validated layout by principal scope, dataset, structural query identity, and revision. An unknown revision produces an explicit retry response; the browser resends the manifest. Redis is optional. Every data response carries the revision that ordered both schema and cells. The browser discards a response for an older revision. Layout revisions join all schema, viewport, and export cache keys.

The server resolves the scrolling display window and pinned identities together. Widths stay browser-owned. Saved views persist order, widths, and pinning separately. Clipboard and export use the same server resolver, then apply their own visibility and scope options.

## Bounds and correctness

Limit manifest bytes, item count, pinned identity count, and resolved visible window width. A layout revision is a deterministic digest of normalized manifest plus structural query identity, not an authorization capability. On each worker, the resolver rechecks dataset and column authorization. Cache misses and restarts retry through the manifest path. Responses for two layout revisions must never share schema or cells.

## Verification

Test duplicate and unknown identities, temporarily missing and returning values, new columns, grouping, pinned plus scrolling fetch, stale responses, cache misses across workers/restarts, clipboard/export ordering, and browser journeys in Dash, React, and Streamlit with wide sparse schemas.
