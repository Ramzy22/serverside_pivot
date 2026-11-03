# Chart Float lifecycle fix

qualificationEvidence: false

Base: e564675. Isolated worktree: complete207-chart-float.

## Architectural cause

The core stage instantiated useRuntimeChartFloatingInteractions with updateChartCanvasPane taken from its stage snapshot. That command did not yet exist: useRuntimeChartCanvasCommands creates it in the later command stage. The floating leaf destructures its named dependencies and captures the undefined command in its callbacks. Publishing the real update command downstream cannot repair that earlier closure. The presentation's Float callback therefore throws TypeError: updateChartCanvasPane is not a function, leaving the actual pane docked. The same missing dependency affects floating drag and resize.

## Production change

Move the existing floating-interaction composition, unchanged, from useCoreRuntimeStage.tsx to useCommandRuntimeStage.tsx immediately after materializing chartCanvasCommands. The core retains state, refs and sizing ownership; the command stage now owns gestures which require its pane commands. Named dependencies and immutable one-direction domain publication remain intact. No ref bag or late-bound stage lookup was introduced. The saved-report identity fix in the base is unchanged.

## Behavioral regression and red/green evidence

New tests/frontend_chart_float_lifecycle.cjs compiles the actual production chart hook call statements and publication loops from the core and command stages, retaining their production stage order. It runs real useRuntimeChartCanvasCommands, useRuntimeChartFloatingInteractions, normalization and workspace presentation code. Only React scheduling and browser geometry/events use a controlled test harness; no chart command or chart normalization behavior is mocked. Unrelated grid/transport hooks are outside this focused harness.

Before the production move, both behavioral tests failed with updateChartCanvasPane is not a function. The first failure originated from the real docked renderChartCanvasPane element's onToggleFloating property. After the move, both pass:

- Docked Float removes the same pane from the docked grouping and adds it to the floating grouping; the floating panel receives floating:true and retains source and model; Dock returns the same pane to the original left position with its size and source intact.
- Floating drag and resize update the same pane through the command owner, with literal rectangle/width/chart-height assertions and preserved pane identity/source.

A Python wrapper includes this Node regression in normal pytest discovery. The prior structural runtime-owner check was updated to the owner-correct command-stage boundary. Existing pane/source/export/browser lifecycle oracles were not changed.

## Verification

- node --test tests/frontend_chart_float_lifecycle.cjs: RED before fix (2 failures, captured undefined command), GREEN after fix (2 passed).
- npm run typecheck (frontend): PASS, including checked-JS injected TS2322 gate.
- python -m pytest tests/test_chart_float_lifecycle_frontend.py tests/test_frontend_runtime_ownership.py tests/test_runtime_contracts.py -q: 104 passed.
- node tests/frontend_flagship_charts.cjs: PASS.
- node tests/frontend_saved_view_persistence.cjs: PASS.
- git diff --check: PASS.

No full suite, browser, application workflow or clipboard operation was run by this worker. This report is source/regression evidence only and does not qualify the installed candidate. The parent must build/install the resulting candidate and rerun the exact pane/source Float lifecycle oracle in the serialized host qualification workflow.
