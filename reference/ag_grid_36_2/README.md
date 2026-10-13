# Local AG Grid SSRM reference

This isolated app pins AG Grid Enterprise and Community to 36.2.0. It reads the same UTF-8 JSON fixture as the product's `PIVOT_FLAGSHIP_PARITY_DATA=1` mode. AG Charts Enterprise follows its separate 14.2.0 version line. The backend serves grouping, pivot aggregation, filters, sorting, and row windows through AG Grid's actual Server-Side Row Model datasource. Set Filter choices are served by `/api/filter-values` for the active other-column filter context each time a filter opens.

From this directory, run `npm ci`, then `npm run dev` and open `http://127.0.0.1:4179/`. `npm test` checks the deterministic SSRM processor and HTTP trace. `npm run build` creates a local-only production bundle; set `NODE_ENV=production` and run `npm start` to serve that bundle. The app exposes `/reference-config.json` (including SHA-256 of the raw fixture bytes) and `/request-trace.json` (each `getRows` request and response).

No licence key is stored or set. The AG Grid Enterprise watermark and licence console warning are expected during local evaluation and are the only allowlisted diagnostics. They do not excuse other browser errors. Do not publish this app or its generated `dist/` bundle with the product. The reference is outside both product packaging roots, and `dist/` and `node_modules/` are ignored.

The starting layout groups by region and account, pivots period, sums the three measures, and filters scope to Included. The extra text columns carry tab, LF, CRLF, Unicode, null, and empty-string copy cases; switch off pivot mode to select those raw cells. Export and clipboard behavior under SSRM must be observed in a browser before claiming parity, including whether unloaded rows are included. This app does not itself certify those behaviors.
