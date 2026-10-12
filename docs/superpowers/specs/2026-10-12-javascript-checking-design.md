# JavaScript checking migration

## Intent and boundary

Make TypeScript check the remaining JavaScript implementation and make that check a required release step. Do not call the project fully JavaScript-checked until every supported source module is included and the full check passes. This is separate from production qualification, which still requires release-bound load, Redis failover, browser, dependency-audit, deployment, and shared-report evidence.

The existing `dash_tanstack_pivot/tsconfig.json` enables `allowJs` but sets `checkJs: false` and includes TypeScript entry points. An exploratory strict `checkJs` run reported 6,855 diagnostics across 127 of 134 JS files. Disabling only `noImplicitAny` still left 2,024 diagnostics. These counts describe the current working tree and toolchain, not a permanent acceptance threshold.

## Chosen approach

Use a dedicated, strict JavaScript-checking project with an explicit checked-file manifest. Add it to `npm run typecheck` and the release gate immediately, starting with six small JS modules whose isolated compiler project can pass strict checking with minimal JSDoc repairs. The package entry point imports unchecked modules and cannot be admitted independently. Expand the manifest domain by domain after resolving each batch's diagnostics. TypeScript source checking remains in place. The final batch removes the subset boundary and turns on `checkJs` for the entire supported `src/lib` tree; only then may documentation claim complete JavaScript checking.

Avoid broad `@ts-nocheck`, generated diagnostic baselines, or relaxing strictness to make a batch appear green. Use JSDoc types, declaration boundaries for third-party APIs, or conversion to TypeScript where that makes ownership clearer. A narrow `@ts-expect-error` is permitted only with a local explanation and a test of the runtime contract.

## Alternatives considered

- Enable `checkJs` globally now: maximizes immediate coverage, but leaves the existing required typecheck red on thousands of unrelated diagnostics and blocks every release.
- Suppress existing files and call the switch complete: preserves a green command but offers no meaningful check for most JavaScript and hides migration progress.
- Gate a growing explicit subset (selected): every included file is genuinely checked, releases remain buildable, and the unchecked scope is measurable and visible.

## Contract and batches

The checked manifest is reviewed code, not generated from whichever files happen to compile. The first batch is `runtimeResponseDedup.js`, `formatting.js`, `Icons.js`, `useRuntimeExportState.js`, `useCellInteraction.js`, and `rawTableTsv.js`. The initial `virtualAxes.js` and `DrillThroughModal.js` candidates had 58 diagnostics when isolated as roots, so they move to later batches rather than weakening strictness. Subsequent batches prioritize data/transport and table behavior, then workspace and optional presentation/chart modules. Imported source dependencies are also within the compiler's effective project; a batch is accepted only when the command succeeds without suppressing those dependencies.

Each batch records: included JS modules, remaining JS modules, diagnostic counts by error code, and tests exercising the touched behavior. CI runs both the existing TypeScript check and the checked-JS project on every change. A test verifies that a deliberate type error in an admitted JS fixture makes the release command fail, so a misconfigured include cannot silently turn the gate into a no-op.

## Completion criteria

1. `npm run typecheck` fails on type errors in every supported JS and TS source file and passes on the release commit.
2. No blanket suppressions or unchecked source directories remain in the shipping frontend.
3. Dash, React, and Streamlit builds and current browser flows pass from that same commit; the generated host assets correspond to the checked source.
4. Release notes distinguish this code-quality gate from deployment and collaboration qualification, which require independent evidence.
