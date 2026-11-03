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


## Independent review corrections after 5b39367

Addressed both findings from unloaded-range-product-review.md. Context-menu promotion now calls the same scoped stable capture callback used by mouse/keyboard selection. That callback is an internal hook-to-stage port; public component props are unchanged. Audited concrete setLastSelected/setDragStart producers in the existing scoped product files: mouse, keyboard and select-all use captureAnchor; viewport right-click now does too; other setters clear anchors. useCellInteraction initializes typed null state; useRenderHelpers only reads anchors. The separate row-selection domain remains outside this correction.

Semantic scope invalidation now clears isFilling and fillRange with dragStart. Fill completion also requires a present anchor whose captured scope matches the current scope, preventing a new-scope event from completing an old operation before reset effects run and preventing null-anchor dereference. Editing exclusions and existing fill semantics remain unchanged.

Regression evidence: the actual viewport context-menu promotion source branch is executed up to menu assembly, then fed into the actual selection and clipboard hooks. It checks shifted loaded endpoints and missing-endpoint refusal. On HEAD 5b39367 this test fails because the original endpoint is lost; after correction it passes. The fill regression executes the actual scope reset effect and completion callbacks, checking cleared fill state, no exception and no stale batch update, including null-anchor completion. Both added cases failed before product edits (31 pass, 2 fail); all pass after correction.

Commands/results:
- node --test tests/frontend_clipboard_serialization.cjs tests/frontend_pinned_selection.cjs: 33 passed (23 clipboard including prior 22; 10 pinned including prior 9), 0 failed.
- node --test --test-name-pattern="actual context-menu promotion" tests/frontend_clipboard_serialization.cjs with only viewport source temporarily restored from HEAD inside a try/finally: expected RED exit 1, missing original endpoint assertion. Fixed bytes restored immediately.
- C:/Users/ramzy/AppData/Local/Temp/pf-sec207/Scripts/python.exe -m pytest tests/test_clipboard_serialization.py -q -k shared_clipboard: 1 passed, 6 deselected; existing pytest-asyncio/Dash deprecation warnings.
- npm run typecheck in dash_tanstack_pivot: exit 0; TypeScript and checked-JS gate passed.
- git diff --check: exit 0.

Only the existing two product files, two focused regression files and this report changed. No browsers, native clipboard, broad suite, root source edits or subagents. qualificationEvidence remains false; root fresh review/build/install/native qualification remains required.
