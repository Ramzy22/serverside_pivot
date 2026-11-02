# Unloaded-range product integrity fix

qualificationEvidence: false

Branch: fix-complete207-unloaded-range. Base: 5ccc7f0. Isolated worktree: .worktrees/complete207-unloaded-range.

## Cause and design

The original mouse/keyboard anchors stored only a cache-relative rowIndex and display column index. After a server viewport replaced getDisplayRows, Shift-click resolved that same integer against unrelated current rows. The resulting selection omitted the original endpoint, so the existing clipboard missing-row guard could not detect truncation.

CellSelectionAnchor now carries stable row and column IDs, the captured virtual row coordinate, semantic scope, and the original rendering indices. Capture/resolution helpers have explicit checked JSDoc types; existing useCellInteraction anchor state is typed with this contract. Mouse, keyboard, drag and select-all capture IDs. Present endpoints resolve by ID in actual display order, including column pinning/reordering; hidden column anchors do not silently rebind. Active-cell rendering follows IDs as well.

When an endpoint is absent, selection scans only loaded rows between captured virtual endpoints and retains the absent endpoint's real row ID as a selected cell key. Its value is undefined because it is unavailable. This is bounded by loaded rows and selected columns plus at most two endpoint rows; a trillion-row gap never allocates missing interior rows. The unchanged clipboard presentation guard rejects the real missing ID, or a virtual-index discontinuity when both endpoints remain loaded. No synthetic row IDs, sentinel cells, environment composition, shared state bags, or clipboard oracle changes were introduced.

Semantic scope is supplied through one narrow string input built from server/client mode, dataset tableName, existing sort/filter/row/column/value/custom-dimension cache key, expansion and view/detail/tree/report definitions. Viewport buffers, response/data versions and table-model object identity are excluded. Scope changes clear selection/anchors/drag/select-all state; event-time range resolution also rejects an old scope before an effect can run. Keyboard navigation does not reinterpret an absent anchor as another loaded row.

## Red-green and verification

Before product edits, the actual React hooks (rendered with ReactDOM server, explicit state setter ports between renders) failed all five newly added regressions: replacement window lost original endpoint; retained endpoint at a shifted index was lost; fully loaded shifted range omitted its first row; huge-gap drag lost its endpoint; changed semantic scope extended stale selection. Existing 14 serializer tests passed. Red command used the required clean Python executable, not a broad suite.

Final commands:

- `C:/Users/ramzy/AppData/Local/Temp/pf-sec207/Scripts/python.exe -m pytest tests/test_clipboard_serialization.py -q -k shared_clipboard`: 1 passed, 6 deselected (22 Node subtests inside the wrapper), 3.09s. Existing pytest-asyncio configuration and Dash plugin warnings. Includes actual selection-to-clipboard-hook refusal, Unicode/colon IDs, missing middle, fully loaded exact headers/rows, keyboard shifted/absent anchor, actual scope helper with sort/filter/expansion/dataset changes, and a 1.5-second simulated delivery-port sentinel monitor. This monitor does not touch or qualify the OS clipboard.
- `node --test tests/frontend_pinned_selection.cjs`: 9 passed. Original pinned/gutter/Tab/Shift-arrow/select-all/value-order checks retained; exact anchor shapes expanded to assert stable IDs. Additional actual-hook scope-effect clearing and pinned-reordered/hidden-column tests pass. This harness stubs React primitive callbacks/effects; the explicit scope-reset effect callback is executed.
- `npm run typecheck` in dash_tanstack_pivot: exit 0, full TypeScript plus strict checked-JS and injected TS2322 rejection gate. Executed after typed anchor state and semantic scope helper changes.
- `git diff --check`: passed.

An initial direct Node command used the package directory with a root-relative tests path and failed to find the test; corrected root-directory command above supplies the real result. No broad pytest, browser, server, native input, or real clipboard operations occurred. node_modules is a junction to existing root dependencies.

## Scope and remaining qualification

Product files: useRuntimeCellSelection.js, new cellSelectionAnchor.js, useViewportRuntimeStage.tsx, useCellInteraction.js and useRenderHelpers.js. Test files: frontend_clipboard_serialization.cjs and frontend_pinned_selection.cjs. Clipboard serialization/delivery product and historical artifacts/source archive remain untouched. Fill/edit behavior was not expanded.

Root must review, freshly build/install this source and rerun serial native unloaded-range qualification in Dash/React/Streamlit, including exact refusal and unchanged OS clipboard for 1.5 seconds. Source regressions and the simulated sentinel are not native acceptance evidence. qualificationEvidence remains false until that run.
