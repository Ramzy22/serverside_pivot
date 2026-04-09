# Edit System Architecture Plan

## 1. Goals

1. **Single source of truth** — Replace the 3 parallel Maps (`optimisticCellValues`, `comparisonCellOriginalValues`+`comparisonCellCounts`, `editedCellMarkers`) with one unified `CellEditStore`.
2. **Zero copy-paste** — Extract the descendant/ancestor walk (duplicated 6 times) into a single `treeWalker` module.
3. **Reverts are undoable** — Every revert pushes to the undo stack like any other edit.
4. **Grand total gets propagation picker** — Fix the `isParentAggregatePropagationEdit` exclusion.
5. **Clean hook extraction** — Move ~1,500 lines of edit state + logic out of the 11k-line `DashTanstackPivot.react.js` into `useEditEngine`.
6. **Stable undo/redo** — Store affected-cell snapshots that survive tree shape changes, not stale marker plans.
7. **Single persistence path** — Replace the 3-flag restore dance with one deterministic restore function.

Non-goals: no changes to the Python server-side transaction engine, no changes to the `editing.js` config/validation utils, no changes to the `EditSidePanel` visual design (only its prop contract).

**Revised goals from logic audit**:
- Reverts use the server's `inverseTransaction`, never a client-constructed approximation
- Revert deduplicates history entries across overlapping edit regions
- Client-only mode (non-server-side) handled consistently  
- Graceful fallback when server does not return a capturable inverse

---

## 2. File Structure

```
src/lib/hooks/useEditEngine/
  index.js                 ← main hook, composes everything, only public export
  cellEditStore.js         ← unified Map store + operations
  treeWalker.js            ← ancestor/descendant walking (one implementation)
  optimisticValues.js      ← per-request optimistic value tracking + ancestor rollup
  transactionHistory.js    ← undo/redo stack, pending request tracking
  propagationGate.js       ← propagation method selection state machine
  editSerializer.js        ← serialize/restore for Dash prop + sessionStorage + persistence
  constants.js             ← GRAND_TOTAL_ROW_ID re-export, max history, key helpers
```

Everything else stays where it is:
- `utils/editing.js` — config normalization, validation, editor types (unchanged)
- `Table/EditSidePanel.js` — presentation component (props simplified)
- `scalable_pivot_controller.py` + `tanstack_adapter.py` — server (unchanged)

---

## 3. Data Model

### 3.1 CellEditStore (`cellEditStore.js`)

One `Map<cellKey, CellEditEntry>`. The cell key is `"${rowId}:::${colId}"` (same as today).

```js
CellEditEntry = {
  rowId:            string,
  colId:            string,
  originalValue:    any,       // captured on FIRST touch, never overwritten
  directCount:      number,    // +1 per edit that directly targets this cell
  propagatedCount:  number,    // +1 per edit that affects this cell via propagation
}
```

**Invariant**: An entry exists in the Map if and only if `directCount > 0 || propagatedCount > 0`. When both reach 0, the entry is deleted.

**`originalValue` semantics**: Set once, when the entry is first created (count goes from 0 to 1). Never updated by subsequent edits. This is the revert target. On undo, when counts return to 0 and the entry is deleted, the original value is lost — which is correct, because the cell is back to its unedited state.

**Why counts instead of booleans**: Overlapping edit regions. If Edit A propagates to cell X (`propagatedCount=1`) and then Edit B directly edits cell X (`directCount=1`), undoing Edit A decrements `propagatedCount` to 0 but the cell still shows as "direct" because `directCount=1`. Without counts, undoing A would incorrectly remove the entire entry.

#### Store Operations

```js
class CellEditStore {
  // --- Core mutations (called by actions, never by components) ---

  /** Apply a set of affected cells in forward or backward direction. */
  applyAffectedCells(affectedCells, direction, resolveValue)
    // affectedCells = { direct: Map<key, {rowId, colId}>, propagated: Map<key, {rowId, colId}> }
    // direction = 'forward' | 'backward'
    // resolveValue = (rowId, colId) => currentValue  (for capturing originalValue on first touch)
    //
    // forward:  increment matching count, create entry if new, capture originalValue
    // backward: decrement matching count, delete entry if both counts reach 0

  /** Hard-clear specific keys (for revert paths). */
  deleteKeys(keys)

  /** Clear everything (for revert-all, table/serverSide change). */
  clear()

  // --- Queries (called by components during render) ---

  /** Returns { direct: bool, propagated: bool } or null if cell is unedited. */
  getMarker(cellKey)

  /** Returns the originalValue for a cell, or undefined if unedited. */
  getOriginalValue(cellKey)

  /** Returns true if any entries exist. */
  get hasEdits()

  /** Returns the number of entries. */
  get size()

  /** Iterate all entries (for serialization, panel display). */
  forEach(callback)

  // --- Epoch (trigger React re-renders) ---

  get epoch()     // monotonic counter
  bump()          // increment epoch
}
```

The store is a plain object with a `Map` inside, held in a `useRef`. The `epoch` is a separate `useState` counter that components depend on for re-renders. The store mutates the Map synchronously (fast), then calls `bump()` once at the end to batch the React update.

### 3.2 OptimisticValues (`optimisticValues.js`)

Separate from the edit store because optimistic values are **transient** (cleared when server confirms) while the edit store is **persistent** (survives until undo/revert).

```js
OptimisticValueTracker = {
  values:   Map<cellKey, { requestId, rowId, colId, value }>,
  requests: Map<requestId, Set<cellKey>>,
}
```

Operations:
```js
  capture(updates, requestId, { resolveAggConfig, resolveCurrentValue, getDirectChildRowIds })
    // For each update:
    //   - Store the optimistic value for the edited cell
    //   - For sum/min/max aggregations: compute + store ancestor rollup values
    //     (same logic as current captureOptimisticCellValues, lines 4207-4278)

  releaseRequest(requestId)
    // Mark request as confirmed — stop tracking keys but don't delete values yet
    // (values are cleaned up lazily by reconcileWithPayload)

  clearRequest(requestId)
    // Error path — delete all optimistic values for this request

  clearAll()

  reconcileWithPayload(responseRows, resolveRowId)
    // When server data arrives, delete optimistic entries where server value matches

  resolve(cellKey, fallbackValue)
    // Return optimistic value if present, else fallbackValue
```

This is essentially the same as today's `optimisticCellValuesRef` + `optimisticCellRequestsRef`, just encapsulated.

### 3.3 TransactionHistory (`transactionHistory.js`)

```js
HistoryEntry = {
  id:                  string,       // unique ID for this entry
  kind:                'transaction', 
  source:              string,       // 'inline-edit' | 'row-edit' | 'revert' | 'reapply' | ...
  createdAt:           number,
  inverseTransaction:  object,       // from server response — complete row-level inverse
  redoTransaction:     object,       // from server response — complete row-level redo
  affectedCells:       { direct: string[], propagated: string[] },  // cell keys
  propagationStrategy: string|null,  // 'equal' | 'proportional' | 'none' — what the user picked
  serverPropagation:   object|null,  // full propagation summary from server (aggregationFn, strategy, fromValue, toValue, etc.)
}
```

**Key changes from today**:

1. `affectedCells` stores **cell keys** (strings), not a "plan" object. The store operations use the same forward/backward `applyAffectedCells` method. No separate "marker plan" or "comparison plan" — just one set of affected keys.

2. `inverseTransaction` is the server's exact row-level inverse — not a client-constructed approximation. This is the **only** payload used for undo and revert. The client never fabricates revert updates from `originalValue`.

3. `propagationStrategy` records the method the user chose. Used by re-apply propagation (to know what strategy to invert).

4. `serverPropagation` records the full propagation summary (matchedRowCount, updatedRowCount, strategy, fromValue, toValue). Used to display in the propagation log and to validate that re-apply is safe.

**No `originalValues` snapshot**: The plan does NOT store per-cell original values in the history. The `CellEditStore` is the sole owner of original values. The history entry's `inverseTransaction` is what restores data on undo — it's authoritative because the server computed it from the real before/after row state, not from the client's approximation.

```js
class TransactionHistory {
  #undoStack = []
  #redoStack = []
  #pending = Map<requestId, PendingEntry>
  #maxEntries = 100

  /** Register a dispatched request that hasn't been confirmed yet. */
  registerPending(requestId, pendingEntry)

  /** Called when server responds. Resolves the pending entry. */
  finalizePending(requestId, serverResult) → { entry, action }

  /** Called on error. Rolls back the pending entry. */
  rejectPending(requestId)

  /** Push a confirmed entry onto the undo stack. */
  push(entry)

  /** Pop for undo. Returns the entry to undo, moves to redo stack. */
  popUndo() → HistoryEntry | null

  /** Pop for redo. Returns the entry to redo, moves to undo stack. */
  popRedo() → HistoryEntry | null

  /** 
   * Collect all entries in the undo stack that directly edited a cell.
   * Returns them in reverse chronological order (most recent first).
   * Used by revert to know exactly which history entries to undo.
   */
  collectDirectEditsFor(cellKey) → HistoryEntry[]

  /**
   * Remove specific entries from the undo stack (used after revert
   * dispatches their inverse transactions). Also clears redo stack 
   * because selective removal invalidates the redo chain.
   */
  removeEntries(entryIds)

  get undoCount()
  get redoCount()
  get hasPending()

  clear()
}
```

### 3.4 PropagationGate (`propagationGate.js`)

A tiny state machine for the "pick propagation method" flow.

```
States:  idle → pending → idle
```

```js
PropagationGate = {
  state:    'idle' | 'pending',
  updates:  array | null,     // stashed updates waiting for method selection
  source:   string | null,
  meta:     object | null,
  method:   string,           // 'equal' | 'proportional' | 'none'
  lastUsedMethod: string,     // remembers across edits (default 'equal')
}
```

Operations:
```js
  /** Check if any updates need the propagation picker. */
  needsGate(updates, rowFields) → boolean

  /** Stash updates and enter 'pending' state. Opens the panel. */
  stage(updates, source, meta)

  /** User confirmed. Returns the stamped updates and exits 'pending'. */
  confirm(method) → { updates, source, meta }

  /** User cancelled. Clears pending state. */
  cancel()
```

The `isParentAggregatePropagationEdit` check moves here with two fixes:

**Fix 1**: Remove the `GRAND_TOTAL_ROW_ID` exclusion so grand total edits get the propagation picker.

**Fix 2**: Add a **row-count guard** for grand total edits. When `rowId === GRAND_TOTAL_ROW_ID`, the server's `key_columns` is empty → `WHERE TRUE` → updates every row in the table. For large datasets this is catastrophic. The gate should:
- Show a confirmation warning: "This will modify all {N} source rows in the table."
- Require explicit confirmation (separate from the propagation method picker)
- Block the edit entirely if row count exceeds a configurable threshold (default: 100,000)

The row count is available from the parent's `rowCount` prop (server-side) or `data.length` (client-side). Pass it into the gate as context.

### 3.5 TreeWalker (`treeWalker.js`)

**Single implementation** of every ancestor/descendant walk. Currently duplicated in:
- `buildEditedCellMarkerPlan` (line 4397)
- `buildComparisonValuePlan` (line 4469)
- `captureOptimisticCellValues` (line 4207)
- `handleRevertCell` (line 9919)
- `onRevertSelected` inline handler (line 10729)
- `onReapplyPropagation` inline handler (line 10662)

```js
/** Yield ancestor rowIds from child up to (and including) grand total. */
function* walkAncestors(rowId) {
  if (!rowId || rowId === GRAND_TOTAL_ROW_ID) return;
  const parts = rowId.split('|||');
  for (let depth = parts.length - 1; depth >= 1; depth--) {
    yield parts.slice(0, depth).join('|||');
  }
  yield GRAND_TOTAL_ROW_ID;
}

/** Yield visible descendant rowIds under an ancestor. */
function* walkVisibleDescendants(ancestorRowId, visibleRows, resolveRowId) {
  if (!ancestorRowId) return;
  for (const row of visibleRows) {
    const id = resolveRowId(row);
    if (!id || id === ancestorRowId) continue;
    if (ancestorRowId === GRAND_TOTAL_ROW_ID) {
      if (id !== GRAND_TOTAL_ROW_ID) yield id;
    } else if (id.startsWith(`${ancestorRowId}|||`)) {
      yield id;
    }
  }
}

/** 
 * Build the complete set of affected cells for a batch of updates.
 * Returns { direct: Map<key, {rowId, colId}>, propagated: Map<key, {rowId, colId}> }
 */
function buildAffectedCells(updates, {
  visibleRows,
  resolveRowId,
  resolveAggConfig,
  cellKey,     // (rowId, colId) => string
}) {
  const direct = new Map();
  const propagated = new Map();

  const mark = (rowId, colId, kind) => {
    const k = cellKey(rowId, colId);
    if (!k) return;
    if (kind === 'direct') {
      direct.set(k, { rowId, colId });
      propagated.delete(k);    // direct supersedes propagated
    } else if (!direct.has(k)) {
      propagated.set(k, { rowId, colId });
    }
  };

  for (const update of updates) {
    const rowId = update.rowPath || update.rowId;
    const colId = update.colId;
    mark(rowId, colId, 'direct');

    const agg = update.aggregation || resolveAggConfig(colId);
    const aggFn = agg?.agg?.trim().toLowerCase() || '';
    if (!aggFn || agg?.windowFn) continue;

    const strategy = (update.propagationStrategy || '').trim().toLowerCase();

    // Ancestors: always propagated
    for (const ancestorId of walkAncestors(rowId)) {
      mark(ancestorId, colId, 'propagated');
    }

    // Descendants: propagated unless strategy is "none"
    if (strategy !== 'none') {
      for (const descId of walkVisibleDescendants(rowId, visibleRows, resolveRowId)) {
        mark(descId, colId, 'propagated');
      }
    }
  }

  return { direct, propagated };
}

/**
 * Collect store entries that are descendants of a given cell.
 * Used by revert to find all cells that need to be reverted together.
 */
function collectStoreDescendants(store, rowId, colId, cellKey) {
  const keys = new Set();
  const k = cellKey(rowId, colId);
  if (k) keys.add(k);

  const prefix = rowId === GRAND_TOTAL_ROW_ID ? null : `${rowId}|||`;
  store.forEach((entry, key) => {
    if (entry.colId !== colId) return;
    if (rowId === GRAND_TOTAL_ROW_ID) {
      keys.add(key);    // grand total revert covers everything
    } else if (prefix && entry.rowId.startsWith(prefix)) {
      keys.add(key);
    }
  });
  return keys;
}
```

### 3.6 EditSerializer (`editSerializer.js`)

**Single serialize/restore pair**. Replaces:
- `serializeEditComparisonState` (line 3765)
- `restoreSerializedEditComparisonState` (line 3808)
- The `editState` prop emit effect (line 4370)
- The `editState` prop restore effect (line 4347)
- The sessionStorage save/load (line 4386)
- The persistence save/load (lines 4337, 4817)
- The 3 restore flags (`didRestoreEditStateFromPropRef`, `didRestorePersistedEditComparisonRef`, `didAttemptEditComparisonRestoreRef`)

```js
/**
 * Serialize the edit engine state into a plain object
 * suitable for Dash prop, sessionStorage, or localStorage.
 */
function serialize(store, displayMode, propagationLog) {
  const cells = [];
  store.forEach((entry, key) => {
    cells.push({
      key,
      rowId: entry.rowId,
      colId: entry.colId,
      originalValue: entry.originalValue,
      direct: entry.directCount > 0,
      propagated: entry.propagatedCount > 0,
    });
  });

  if (cells.length === 0 && propagationLog.length === 0) return null;

  cells.sort((a, b) => a.key.localeCompare(b.key));
  return { version: 2, cells, displayMode, propagationLog };
}

/**
 * Restore from a serialized state. Returns the non-store fields.
 * Mutates the store in place.
 */
function restore(serialized, store) {
  store.clear();
  if (!serialized || typeof serialized !== 'object') return null;

  // Support v1 format (current) with best-effort migration
  const cells = Array.isArray(serialized.cells) ? serialized.cells : [];
  const markers = Array.isArray(serialized.markers) ? serialized.markers : [];

  if (serialized.version === 2) {
    // New format: cells have all the data
    for (const cell of cells) {
      store.set(cell.key || `${cell.rowId}:::${cell.colId}`, {
        rowId: cell.rowId,
        colId: cell.colId,
        originalValue: cell.originalValue,
        directCount: cell.direct ? 1 : 0,
        propagatedCount: cell.propagated ? 1 : 0,
      });
    }
  } else {
    // v1 migration: cells have originalValue, markers have direct/propagated
    const markerMap = new Map();
    for (const m of markers) {
      markerMap.set(m.key, { direct: !!m.direct, propagated: !!m.propagated });
    }
    for (const cell of cells) {
      const key = `${cell.rowId}:::${cell.colId}`;
      const marker = markerMap.get(key) || { direct: false, propagated: false };
      if (!marker.direct && !marker.propagated) {
        // Have original value but no marker — treat as propagated
        marker.propagated = true;
      }
      store.set(key, {
        rowId: cell.rowId,
        colId: cell.colId,
        originalValue: cell.originalValue,
        directCount: marker.direct ? 1 : 0,
        propagatedCount: marker.propagated ? 1 : 0,
      });
    }
  }

  return {
    displayMode: serialized.displayMode || 'edited',
    propagationLog: Array.isArray(serialized.propagationLog) ? serialized.propagationLog : [],
  };
}

/**
 * Deterministic restore priority. Called once on mount.
 * Priority: externalEditState prop > sessionStorage > persistence (localStorage).
 * Returns the resolved state or null.
 */
function resolveInitialState(externalEditState, sessionKey, loadPersisted) {
  // 1. Dash prop (authoritative if present)
  if (externalEditState && typeof externalEditState === 'object'
      && (Array.isArray(externalEditState.cells) || Array.isArray(externalEditState.markers))) {
    return externalEditState;
  }
  // 2. sessionStorage (survives page refresh within tab)
  if (sessionKey) {
    try {
      const raw = sessionStorage.getItem(sessionKey);
      if (raw) return JSON.parse(raw);
    } catch { /* ignore */ }
  }
  // 3. Persistence (localStorage, survives across sessions)
  const persisted = loadPersisted?.('editComparisonState', null);
  if (persisted && typeof persisted === 'object') return persisted;

  return null;
}
```

---

## 4. The Main Hook: `useEditEngine` (`index.js`)

### 4.1 Signature

```js
export function useEditEngine({
  // --- Identity ---
  id,                          // component ID (for sessionStorage key)

  // --- Mode ---
  serverSide,                  // boolean
  rowFields,                   // string[]
  colFields,                   // string[]

  // --- Data access (refs/callbacks from parent) ---
  visibleRowsRef,              // ref to current tableData array
  resolveRowId,                // (row) => string | null
  resolveAggConfig,            // (colId) => { field, agg, weightField, windowFn } | null
  resolveCurrentCellValue,     // (rowId, colId) => any (from table data + current optimistic)
  getDirectChildRowIds,        // (ancestorRowId) => string[]

  // --- Server dispatch ---
  dispatchTransactionRequest,  // (payload, source) => { requestId } | null
  emitRuntimeRequest,          // (kind, payload) => void

  // --- Dash integration ---
  setPropsRef,                 // ref to setProps
  externalEditState,           // editState prop value

  // --- Persistence ---
  persistence,                 // boolean
  loadPersistedState,          // (key, default) => any
  savePersistedState,          // (key, value) => void

  // --- UI state from parent ---
  selectedCells,               // { [selKey]: value } — current cell selection

  // --- Feature flags ---
  supportsPatchRefresh,        // boolean — can use patch refresh mode

  // --- Notifications ---
  showNotification,            // (message, tone) => void
})
```

### 4.2 Internal State

```js
// Refs (mutable, synchronous)
const storeRef     = useRef(new CellEditStore());
const optimistic   = useRef(new OptimisticValueTracker());
const history      = useRef(new TransactionHistory());
const propagation  = useRef(new PropagationGate());
const didRestore   = useRef(false);

// React state (triggers re-renders)
const [storeEpoch, bumpStore]             = useReducer(c => c + 1, 0);
const [optimisticEpoch, bumpOptimistic]   = useReducer(c => c + 1, 0);
const [historyEpoch, bumpHistory]         = useReducer(c => c + 1, 0);
const [displayMode, setDisplayMode]       = useState('edited');
const [propagationLog, setPropagationLog] = useState([]);
const [panelOpen, setPanelOpen]           = useState(false);
const [propagationMethod, setMethod]      = useState('equal');
const [pendingPropagation, setPending]    = useState(null);  // for UI reactivity
```

### 4.3 Restore (single useEffect, runs once)

```js
useEffect(() => {
  if (didRestore.current) return;
  didRestore.current = true;

  const sessionKey = id ? `__pivot_editState_${typeof id === 'string' ? id : JSON.stringify(id)}` : null;
  const initial = resolveInitialState(externalEditState, sessionKey, loadPersistedState);
  if (!initial) return;

  const restored = restore(initial, storeRef.current);
  if (!restored) return;

  setDisplayMode(restored.displayMode);
  setPropagationLog(restored.propagationLog);
  bumpStore();
}, []);  // deliberate empty deps — runs once
```

### 4.4 Emit (single useEffect, runs on change)

```js
const serialized = useMemo(
  () => serialize(storeRef.current, displayMode, propagationLog),
  [storeEpoch, displayMode, propagationLog]
);
const serializedKey = useMemo(() => JSON.stringify(serialized), [serialized]);
const lastEmittedRef = useRef(null);

useEffect(() => {
  if (!didRestore.current) return;
  if (serializedKey === lastEmittedRef.current) return;
  lastEmittedRef.current = serializedKey;

  // Emit to Dash
  setPropsRef.current?.({ editState: serialized });

  // Save to sessionStorage
  const sessionKey = id ? `__pivot_editState_${typeof id === 'string' ? id : JSON.stringify(id)}` : null;
  if (sessionKey) {
    try {
      if (serialized) sessionStorage.setItem(sessionKey, JSON.stringify(serialized));
      else sessionStorage.removeItem(sessionKey);
    } catch { /* ignore */ }
  }

  // Save to persistence
  if (persistence) savePersistedState('editComparisonState', serialized);
}, [serializedKey]);
```

**This replaces 5 useEffects and 3 boolean flags with 2 simple effects.**

### 4.5 Shared Helpers (internal to hook)

```js
const cellKey = useCallback(
  (rowId, colId) => {
    if (rowId == null || colId == null) return null;
    return `${String(rowId)}:::${String(colId)}`;
  }, []
);

const getAffectedCells = useCallback(
  (updates) => buildAffectedCells(updates, {
    visibleRows: visibleRowsRef.current || [],
    resolveRowId,
    resolveAggConfig,
    cellKey,
  }),
  [resolveRowId, resolveAggConfig, cellKey]
);

const applyForward = useCallback(
  (affected) => {
    storeRef.current.applyAffectedCells(affected, 'forward', resolveCurrentCellValue);
    bumpStore();
  },
  [resolveCurrentCellValue]
);

const applyBackward = useCallback(
  (affected) => {
    storeRef.current.applyAffectedCells(affected, 'backward', resolveCurrentCellValue);
    bumpStore();
  },
  [resolveCurrentCellValue]
);
```

---

## 5. Actions (the public API)

### 5.1 `applyInlineEdit(update)`

Single-cell edit from double-click or keyboard.

```
1. Check propagation gate:
   - If needsGate([update], rowFields):
       propagation.stage([update], 'inline-edit', null)
       setPending(propagation.pendingUpdates)
       setPanelOpen(true)
       return   ← caller aborts, user picks method later
   - Else: proceed

2. Switch displayMode to 'edited' if currently 'original'

3. If !serverSide (client-only mode):
   - setProps({ cellUpdates: [update] })
   - Build affected cells, apply forward to store
   - return

4. Build affected cells: affected = getAffectedCells([update])

5. Apply forward to store: applyForward(affected)

6. Capture optimistic values: optimistic.capture([update], ...)

7. Dispatch transaction:
   dispatched = dispatchTransactionRequest({
     update: [update],
     refreshMode: supportsPatchRefresh ? 'patch' : 'smart',
   }, 'inline-edit')

8. If dispatch failed: roll back store + optimistic. return.

9. Register pending in history:
   history.registerPending(dispatched.requestId, {
     action: 'apply',
     source: 'inline-edit',
     affectedCells: { direct: [...affected.direct.keys()], propagated: [...affected.propagated.keys()] },
     originalValues: snapshotOriginalValues(storeRef.current, affected),
   })
```

### 5.2 `applyRowEdit(rowId, session)`

Row-level commit (all dirty columns at once). Same flow as `applyInlineEdit` but builds multiple updates from `session.drafts`. Includes validation step (same as current `saveRowEditSession`). On success, closes the row edit session.

### 5.3 `confirmPropagation(method)`

Called when user picks Equal/Proportional/None and clicks "Apply Edit".

```
1. result = propagation.confirm(method)
   - Returns null if not pending → return
   - Returns { updates (with propagationStrategy stamped), source, meta }

2. setPending(null)
3. setPanelOpen(false)

4. Proceed with same steps 2-9 from applyInlineEdit, using result.updates
```

### 5.4 `cancelPropagation()`

```
1. propagation.cancel()
2. setPending(null)
```

### 5.5 `revertCell(rowId, colId)` — History-Based Revert

The revert path is the most critical logic fix. The old approach (build updates from `originalValue`) is broken because:
- It doesn't know the propagation strategy → server defaults to "equal" → child rows get wrong values
- `deleteKeys` bypasses the count system → destroys overlapping edit tracking
- It's not undoable

**New approach: revert = dispatch the history entry's `inverseTransaction`.**

```
1. Find the cell key: k = cellKey(rowId, colId)

2. Collect all history entries that directly edited this cell:
   entries = history.collectDirectEditsFor(k)
   - Returns entries in reverse chronological order (most recent first)
   - Only entries where k is in affectedCells.direct (not propagated)

3. If entries is empty:
   FALLBACK — cell was restored from persistence (no history available).
   Use the legacy approach: send originalValue to server as a new transaction
   with propagationStrategy explicitly set to the last known strategy.
   Show a warning: "Revert may be approximate — edit history not available."
   return

4. For each entry (most recent first):
   a. Dispatch entry.inverseTransaction to server
      - This is the server's exact row-level inverse, computed at edit time
      - It restores every affected child row to its exact before-edit value
      - For proportional edits, this means each row goes back to its own original
   
   b. Roll back the store using count decrements (NOT deleteKeys):
      store.applyAffectedCells(entry.affectedCells, 'backward', ...)
      - direct cell: directCount decrements
      - propagated cells: propagatedCount decrements  
      - If both counts reach 0, entry is deleted (and originalValue is freed)
      - If one count remains > 0 (overlapping edit), entry survives correctly
   
   c. Clear optimistic values for this entry's request

5. Remove the undone entries from the undo stack:
   history.removeEntries(entries.map(e => e.id))
   - Also clears the redo stack (selective removal invalidates redo chain)

6. bumpStore(), bumpHistory()
```

**Why this fixes the flaws:**
- **Flaw 1 (wrong child values)**: The server's `inverseTransaction` contains the exact before-state of every affected row. No approximation.
- **Flaw 2 (overlapping edits)**: `applyBackward` decrements counts instead of hard-deleting. Other edits' counts survive.
- **Flaw 3 (not undoable)**: The entries are removed from the undo stack. This is a destructive revert, but it's correct — the server data has been restored, and the redo chain would be invalid.

**Edge case — multiple stacked edits on the same cell:**

```
Edit A: "USA" sum 1000→1200, proportional → children scaled 1.2x
Edit B: "USA" sum 1200→1500, equal → children each +100

history.collectDirectEditsFor("USA:::Revenue_sum") = [B, A]  (newest first)

Revert processes B first:
  - Dispatches B.inverseTransaction → server undoes the +100/child
  - store: "USA" directCount 2→1

Revert then processes A:
  - Dispatches A.inverseTransaction → server undoes the 1.2x scale
  - store: "USA" directCount 1→0 → entry deleted

Result: all children back to exact original values. ✓
```

### 5.6 `revertSelected(cells)`

Same as `revertCell` but collects history entries for ALL selected cells, deduplicates by entry ID, then processes them most-recent-first. One batch of inverse dispatches.

### 5.7 `revertAll()`

Two sub-strategies:

**If history is available** (normal case):
```
1. Collect ALL entries from the undo stack (oldest first = correct undo order is newest first)
2. For each entry (newest first):
   a. Dispatch entry.inverseTransaction
   b. applyBackward on the store
3. history.clear()
4. optimistic.clearAll()
5. setPropagationLog([])
6. bumpStore(), bumpHistory()
```

This is a full sequential undo. The server processes each `inverseTransaction` in order, restoring exact row values. **Correct for all aggregation types and propagation strategies.**

**If history is empty** (restored from persistence, or very long session where old entries were evicted):
```
1. Collect originalValues from the store
2. Send them to server grouped by aggregation type, with explicit propagationStrategy: 'none'
   - strategy 'none' tells the server "just set this parent value, don't distribute to children"
   - This prevents the server from applying a default equal distribution
   - The parent values will be wrong (they're aggregates, not source data), but they'll
     self-correct on the next viewport refresh when the server recomputes from real data
3. Clear store, optimistic, history, propagation log
4. showNotification('Reverted all tracked edits. Values will refresh from server.', 'info')
```

**Why not just send originalValues with equal strategy?** Because for avg/weighted_avg, the "original value" in the store is the original aggregate (e.g., avg=50). Sending `{value: 50, strategy: equal}` would shift all children by `(50 - currentAvg)`, which is wrong if some children were independently edited. Strategy `'none'` avoids touching children, and the next server refresh recalculates the real aggregate.

### 5.8 `reapplyPropagation(directCells, method)` — Correct Two-Phase Approach

User changes propagation method on already-applied edits (e.g., "equal" → "proportional").

**The key insight**: this is semantically "undo the old propagation, then redo with new method." We must use the history's inverse to undo, not a client-constructed approximation.

```
1. For each direct cell, find its history entry:
   entry = history.collectDirectEditsFor(cellKey(cell.rowId, cell.colId))[0]
   - Most recent entry that directly edited this cell

2. If no entry found → fallback to current (approximate) behavior with warning

3. Phase 1 — Undo the old edit:
   a. Dispatch entry.inverseTransaction to server
      - This restores ALL children to exact pre-edit values (correct for any strategy)
   b. applyBackward(entry.affectedCells) on the store
   c. Remove entry from history

4. Phase 2 — Re-apply with new method:
   a. Build new update: { rowId, colId, value: cell.currentValue, oldValue: entry's fromValue, propagationStrategy: method }
   b. Follow the normal applyInlineEdit path (which goes through propagation gate if needed,
      builds affected cells, dispatches to server, registers in history)

5. The server processes Phase 1 (inverse) then Phase 2 (new edit) in the same request batch.
   Children are first restored to originals, then re-distributed with the new strategy.
```

**Why this fixes Flaw 3 (re-apply undo)**:
- Phase 1 and Phase 2 are separate history entries.
- Undoing the Phase 2 edit dispatches its own `inverseTransaction`, which restores children to their pre-Phase-2 state = the original values (because Phase 1 already restored them).
- The undo chain is clean: undo re-apply → children back to original → user can redo with different strategy if desired.

**Optimization**: Phases 1 and 2 can be sent as a single server request with both the inverse transaction and the new forward edit. The server processes them atomically within one `transaction_ctx`. The history records Phase 2 only (since Phase 1's entry was removed). This avoids two round-trips.

### 5.9 `undo()` / `redo()`

```
undo:
  1. entry = history.popUndo()
  2. if !entry → return
  3. Dispatch entry.inverseTransaction to server
  4. Register as pending with action='undo'
  5. On server confirm:
     - applyBackward(entry.affectedCells)  ← decrement counts
     - optimistic.release(requestId)
     - bumpHistory()

redo:
  1. entry = history.popRedo()
  2. Dispatch entry.redoTransaction
  3. Register as pending with action='redo'
  4. On server confirm:
     - applyForward(entry.affectedCells)  ← increment counts
     - optimistic.release(requestId)
     - bumpHistory()
```

### 5.10 `handleTransactionResponse(response, payload)`

Called by the parent component when a server response arrives. This is the callback that `finalizeTransactionHistoryResponse` currently handles.

```
1. Look up pending entry: history.pending.get(response.requestId)
2. If not found → return
3. Parse server result: didApplyWork, propagationEntries, inverseTransaction, redoTransaction

4. If action === 'apply':
   a. If !didApplyWork:
      - Roll back: applyBackward(entry.affectedCells)
      - optimistic.clearRequest(requestId)
      - Show warning if server gave one
      - return
   b. If didApplyWork:
      - Build confirmed history entry with server's inverseTransaction + redoTransaction
      - history.push(confirmedEntry)
      - optimistic.releaseRequest(requestId)
      - Append propagation log entries
      - If propagation entries exist → setPanelOpen(true)

5. If action === 'undo':
   - applyBackward(entry.affectedCells)
   - optimistic.releaseRequest(requestId)
   - showNotification('Undid the last transaction.', 'success')

6. If action === 'redo':
   - applyForward(entry.affectedCells)
   - optimistic.releaseRequest(requestId)
   - showNotification('Reapplied the last transaction.', 'success')

7. bumpHistory()
```

---

## 6. Return Value (public API surface)

```js
return {
  // --- State for rendering ---
  storeEpoch,              // depend on this to re-render when edits change
  optimisticEpoch,         // depend on this to re-render when optimistic values change
  displayMode,             // 'edited' | 'original'
  panelOpen,               // boolean
  propagationMethod,       // 'equal' | 'proportional' | 'none'
  propagationLog,          // array of log entries
  pendingPropagation,      // array of pending updates (null if none)

  // --- Cell queries (hot path, called per-cell during render) ---
  resolveMarker,           // (rowId, colId) => { direct, propagated } | null
  resolveDisplayValue,     // (rowId, colId, fallback) => displayed value
  resolveOptimisticValue,  // (rowId, colId, fallback) => value with optimistic overlay

  // --- Derived state (for EditSidePanel + StatusBar) ---
  editedCells,             // computed list for the panel (from selectedCells + store)
  historyState,            // { undoCount, redoCount, hasPending }
  hasEdits,                // boolean — any entries in the store

  // --- Actions ---
  applyInlineEdit,         // (update) => void
  applyRowEdit,            // (rowId, session) => void
  revertCell,              // (rowId, colId) => void
  revertSelected,          // (cells) => void
  revertAll,               // () => void
  reapplyPropagation,      // (directCells, method) => void
  undo,                    // () => void
  redo,                    // () => void
  confirmPropagation,      // (method) => void
  cancelPropagation,       // () => void

  // --- Panel control ---
  openPanel,               // () => void
  closePanel,              // () => void
  setDisplayMode,          // (mode) => void
  setPropagationMethod,    // (method) => void

  // --- Lifecycle ---
  handleTransactionResponse,  // (response, payload) => void
  clearAll,                   // () => void (for table/serverSide change)

  // --- Serialized state (for Dash prop emit) ---
  serializedEditState,     // the serialized object (or null)
};
```

---

## 7. Integration with Parent Component

### 7.1 In `DashTanstackPivot.react.js`

Replace the ~1,500 lines (3604-3622 state declarations, 3765-4605 store operations, 4773-4824 persistence, 5128-5543 dispatch functions, 9888-10002 panel data, 10658-10800 inline handlers) with:

```js
const editEngine = useEditEngine({
  id, serverSide, rowFields, colFields,
  visibleRowsRef: tableDataRef,
  resolveRowId: resolveVisibleDataRowId,
  resolveAggConfig: resolveAggregationConfigForColumnId,
  resolveCurrentCellValue,
  getDirectChildRowIds: getDirectVisibleChildRowIds,
  dispatchTransactionRequest,
  emitRuntimeRequest,
  setPropsRef,
  externalEditState,
  persistence,
  loadPersistedState, savePersistedState,
  selectedCells,
  supportsPatchRefresh: supportsPatchTransactionRefresh,
  showNotification,
});
```

Then pass to children:
```js
// useRenderHelpers gets:
resolveEditedCellMarker: editEngine.resolveMarker,
resolveCellDisplayValue: editEngine.resolveDisplayValue,
editedCellEpoch: editEngine.storeEpoch,

// useColumnDefs gets:
onCellEdit: editEngine.applyInlineEdit,

// StatusBar gets:
canUndo: editEngine.historyState.undoCount > 0,
canRedo: editEngine.historyState.redoCount > 0,
canRevertAll: editEngine.hasEdits,
isShowingOriginal: editEngine.displayMode === 'original' && editEngine.hasEdits,
onUndo: editEngine.undo,
onRedo: editEngine.redo,
onRevertAll: editEngine.revertAll,
onToggleOriginal: () => editEngine.setDisplayMode(m => m === 'original' ? 'edited' : 'original'),
```

### 7.2 EditSidePanel New Props

```jsx
<EditSidePanel
  editedCells={editEngine.editedCells}
  propagationLog={editEngine.propagationLog}
  pendingPropagation={editEngine.pendingPropagation}
  propagationMethod={editEngine.propagationMethod}
  displayMode={editEngine.displayMode}
  onRevertCell={editEngine.revertCell}
  onRevertSelected={editEngine.revertSelected}
  onRevertAll={editEngine.revertAll}
  onReapplyPropagation={editEngine.reapplyPropagation}
  onToggleDisplayMode={() => editEngine.setDisplayMode(m => m === 'original' ? 'edited' : 'original')}
  onPropagationMethodChange={editEngine.setPropagationMethod}
  onConfirmPropagation={() => editEngine.confirmPropagation(editEngine.propagationMethod)}
  onCancelPropagation={editEngine.cancelPropagation}
  onClose={editEngine.closePanel}
  theme={theme}
  width={panelWidth}
/>
```

**All handlers are now single function references** — no 70-line inline lambdas in JSX.

### 7.3 Row Edit Sessions

Row edit sessions (`rowEditSessions`, `startRowEditSession`, `updateRowEditSession`, `cancelRowEditSession`, `saveRowEditSession`, `renderRowEditActions`) are **not part of the edit engine**. They manage the in-progress editing UI (which cells are being typed into, draft values, validation errors). They only touch the edit engine at one point: `saveRowEditSession` calls `editEngine.applyRowEdit(rowId, session)`.

These can stay in the parent component or be extracted to a separate `useRowEditSessions` hook in a later phase.

### 7.4 Editor Config

`normalizeEditingConfig`, `resolveColumnEditSpec`, `resolveEditorPresentation`, `requestEditorOptions` — these are about determining WHAT is editable and HOW to render editors. They are not part of the edit engine (which is about tracking WHAT HAS BEEN edited). They stay in the parent or move to their own hook.

---

## 8. Scenario Traces Through New Architecture

### Scenario A: Leaf cell edit ("USA|||CA|||LA" × "Revenue_sum", 100 → 150)

```
1. applyInlineEdit({ rowId: "USA|||CA|||LA", colId: "Revenue_sum", value: 150, oldValue: 100 })
2. PropagationGate.needsGate → false (pathDepth 3 >= groupingDepth 3)
3. getAffectedCells →
     direct:     { "USA|||CA|||LA:::Revenue_sum" }
     propagated: { "USA|||CA:::Revenue_sum", "USA:::Revenue_sum", "__grand_total__:::Revenue_sum" }
4. store.applyAffectedCells(forward):
     "USA|||CA|||LA:::Revenue_sum" → { directCount: 1, originalValue: 100 }
     "USA|||CA:::Revenue_sum"      → { propagatedCount: 1, originalValue: 800 }
     "USA:::Revenue_sum"           → { propagatedCount: 1, originalValue: 1000 }
     "__grand_total__:::Revenue_sum" → { propagatedCount: 1, originalValue: 5000 }
5. optimistic.capture → stores 150 for edited cell, computes 850/1050/5050 for ancestors
6. dispatchTransactionRequest → sends to server
7. history.registerPending with affectedCells + originalValues snapshot
8. Server confirms → history.push(confirmedEntry with inverse/redo from server)
```

### Scenario B: Parent edit with propagation picker ("USA" × "Revenue_sum", 1000 → 1200)

```
1. applyInlineEdit({ rowId: "USA", colId: "Revenue_sum", value: 1200, oldValue: 1000, aggregation: {agg: 'sum'} })
2. PropagationGate.needsGate → true (pathDepth 1 < groupingDepth 3)
3. propagation.stage(updates, 'inline-edit', null)
4. UI shows propagation picker
5. User picks "proportional" → confirmPropagation('proportional')
6. Updates stamped with propagationStrategy: 'proportional'
7. getAffectedCells →
     direct:     { "USA:::Revenue_sum" }
     propagated: { "__grand_total__:::Revenue_sum",
                   "USA|||CA:::Revenue_sum", "USA|||CA|||LA:::Revenue_sum",
                   "USA|||TX:::Revenue_sum", ... (all visible descendants) }
8. Store + optimistic + dispatch as in Scenario A
```

### Scenario C: Grand total edit ("__grand_total__" × "Revenue_sum", 5000 → 6000)

**NEW BEHAVIOR**: PropagationGate.needsGate now returns true for grand total (fix applied). User gets the picker. After confirmation:

```
1. getAffectedCells →
     direct:     { "__grand_total__:::Revenue_sum" }
     propagated: { every other visible row × "Revenue_sum" }
```

### Scenario D: Revert cell ("USA" × "Revenue_sum") — History-Based

Setup: User edited "USA" sum from 1000→1200 with "proportional" strategy. Server scaled children: row1 300→360, row2 700→840.

```
1. revertCell("USA", "Revenue_sum")
2. history.collectDirectEditsFor("USA:::Revenue_sum") → [entryA]
3. Dispatch entryA.inverseTransaction to server
   - This is the server's exact inverse, computed at edit time
   - Contains: row1 360→300, row2 840→700 (exact original per-row values)
   - NOT "delta=-200, distribute equally" which would give 260,740 (wrong!)
4. store.applyAffectedCells(entryA.affectedCells, 'backward'):
   - "USA:::Revenue_sum":           directCount 1→0 → entry DELETED
   - "USA|||CA:::Revenue_sum":      propagatedCount 1→0 → entry DELETED
   - "USA|||CA|||LA:::Revenue_sum": propagatedCount 1→0 → entry DELETED
   - "__grand_total__:::Revenue_sum": propagatedCount 1→0 → entry DELETED
5. history.removeEntries([entryA.id])
6. Server processes inverse → row1=300, row2=700 (exact originals) ✓
```

### Scenario E: Revert with overlapping edits

Setup: Edit A: "USA" 1000→1200, proportional. Edit B: directly edited "USA|||CA|||LA" 360→400.

```
Store state:
  "USA:::Revenue_sum"           → { directCount: 1, propagatedCount: 0 }
  "USA|||CA:::Revenue_sum"      → { directCount: 0, propagatedCount: 1 }
  "USA|||CA|||LA:::Revenue_sum" → { directCount: 1, propagatedCount: 1 }  ← both edits touch this
  "USA|||TX:::Revenue_sum"      → { directCount: 0, propagatedCount: 1 }

User reverts "USA" × "Revenue_sum":
1. history.collectDirectEditsFor("USA:::Revenue_sum") → [entryA]  (Edit A only)
   - Edit B is NOT collected because "USA:::Revenue_sum" is not in B's direct set
2. Dispatch entryA.inverseTransaction → server undoes the proportional scaling
3. store.applyBackward(entryA.affectedCells):
   - "USA:::Revenue_sum":           directCount 1→0 → DELETED
   - "USA|||CA|||LA:::Revenue_sum": propagatedCount 1→0, but directCount still 1 → SURVIVES
   - "USA|||CA:::Revenue_sum":      propagatedCount 1→0 → DELETED
   - "USA|||TX:::Revenue_sum":      propagatedCount 1→0 → DELETED
4. history.removeEntries([entryA.id])

Result: 
  - "USA|||CA|||LA" still shows as "direct" (Edit B's direct count preserved) ✓
  - Server row1 (LA) is at its value after Edit B (400), not the pre-A original (300)
    because the inverse only undid A's proportional scaling on that row
  - All other children back to exact originals ✓
```

### Scenario F: Undo after tree shape change

```
Current approach (broken):
  - markerPlan references child keys visible at edit time
  - After collapse, those keys don't exist → markers stuck or ghost

New approach:
  - history.affectedCells stores key strings
  - undo calls applyBackward → decrements counts
  - If a key isn't in the store (cleaned on collapse), decrement is no-op (max(0, 0-1) = 0)
  - Server's inverseTransaction does the real work (not dependent on client visibility)
  - No stale state ✓
```

### Scenario G: Grand total edit with row-count guard

```
1. User edits "__grand_total__" × "Revenue_sum"
2. PropagationGate.needsGate → true (grand total, groupingDepth > 0)
3. Gate checks rowCount: if > 100,000 → block with warning, return
4. Gate shows propagation picker WITH confirmation:
   "This will modify all 50,000 source rows. Continue?"
5. User confirms with "proportional" → normal flow
```

### Scenario H: Revert when history is unavailable (restored from persistence)

```
1. User refreshes page. Edit store restored from sessionStorage. History is empty.
2. User clicks revert on "USA" × "Revenue_sum"
3. history.collectDirectEditsFor → empty (no history after restore)
4. FALLBACK path:
   - Send originalValue to server with propagationStrategy: 'none'
   - 'none' tells server to only update the parent aggregate cell optimistically
   - Don't touch children (we don't know the right strategy)
   - Parent value will self-correct on next viewport refresh
   - Show warning: "Revert is approximate — full edit history not available."
5. Delete the cell from store (hard delete is OK here — no history = no overlapping concerns)
```

---

## 9. Bug Fix Summary

| # | Current Bug | How New Architecture Fixes It |
|---|------------|------------------------------|
| 1 | 3 Maps must stay in sync | Single `CellEditStore` — one Map, one `applyAffectedCells` |
| 2 | Descendant walks duplicated 6× | Single `treeWalker.js` module with `buildAffectedCells` + `collectStoreDescendants` |
| 3 | Reverts not undoable | Revert dispatches the history entry's `inverseTransaction` and removes the entry from the undo stack. No separate "revert transaction" — revert IS undo of the specific edit. |
| 4 | Revert corrupts child row values (wrong propagation strategy) | Revert uses the server's exact `inverseTransaction` (computed at edit time with correct per-row before/after values), never a client-constructed approximation from `originalValue` with default "equal" strategy. |
| 5 | Revert `deleteKeys` destroys overlapping edit tracking | Revert calls `applyBackward` which decrements counts. Overlapping edit counts survive. Hard delete only used for the no-history fallback path (restored from persistence). |
| 6 | Grand total skips propagation picker | `PropagationGate.needsGate` includes grand total. Row-count guard blocks edits on tables > 100k rows. Confirmation required showing affected row count. |
| 7 | Two revert-all implementations | Single `revertAll()` method. When history available: sequential undo of all entries using server inverses. When history empty: fallback with `propagationStrategy: 'none'` + warning. |
| 8 | Re-apply undo doesn't restore prior propagated state | Re-apply is now two phases: (1) undo old edit via history's `inverseTransaction`, (2) apply new edit with new strategy. Each phase has correct inverse. Undo of Phase 2 restores to post-Phase-1 state = originals. |
| 9 | Undo marker plans stale after tree change | `affectedCells` stores key strings. `applyBackward` is idempotent — missing keys are no-ops. Server `inverseTransaction` is authoritative and visibility-independent. |
| 10 | 3-flag restore dance | Single `resolveInitialState()` with documented priority. One `didRestore` ref. |
| 11 | Off-screen descendants not reverted | Server's `inverseTransaction` operates on `key_columns` (WHERE clause), not on a client-provided list of rowIds. It affects all matching rows regardless of client visibility. |
| 12 | 1,500 lines inline in 11k-line component | Extracted to `useEditEngine` hook with 6 focused modules. |

---

## 10. Migration Plan

### Phase 1: Extract shell (no behavior change)

Create `useEditEngine/index.js` that wraps the existing refs and state. Move all edit-related `useState`/`useRef` declarations into the hook. The hook returns the same values the parent currently uses. The parent destructures them. **Zero functional change** — just code motion.

Estimated diff: ~200 lines added (hook), ~200 lines removed (parent).

### Phase 2: Extract treeWalker

Create `treeWalker.js`. Replace all 6 descendant/ancestor walk sites with calls to `buildAffectedCells` and `collectStoreDescendants`. Verify behavior with existing tests.

### Phase 3: Unify store

Create `cellEditStore.js`. Replace the 3 Maps with one. Update `applyAffectedCells` to operate on the unified store. Update serializer to v2 format with v1 migration. This is the highest-risk phase — needs careful testing.

### Phase 4: Extract optimistic values

Create `optimisticValues.js`. Move `captureOptimisticCellValues`, `releaseOptimisticCellRequest`, `clearOptimisticCellValuesForRequest`, `reconcileOptimisticCellValuesWithPayload` into it. Wire `resolveOptimisticValue` through the hook.

### Phase 5: Extract transaction history

Create `transactionHistory.js`. Move undo/redo stacks, pending tracking, `finalizeTransactionHistoryResponse`, `clearPendingTransactionHistoryRequest` into it. Wire `handleTransactionResponse` through the hook.

### Phase 6: Extract propagation gate + fix grand total

Create `propagationGate.js`. Move `resolvePropagationFormulaUpdates`, `handleConfirmPropagation`, `cancelPropagation` logic into it. Fix grand total exclusion. Move inline `onReapplyPropagation` handler into a named `reapplyPropagation` method.

### Phase 7: Unify persistence

Create `editSerializer.js`. Replace the 3-flag restore logic with `resolveInitialState`. Replace the 2 emit effects with one. Remove `didRestoreEditStateFromPropRef`, `didRestorePersistedEditComparisonRef`, `didAttemptEditComparisonRestoreRef`.

### Phase 8: Make reverts undoable

Update `revertCell`, `revertSelected` to register pending in history. Verify undo-of-revert works correctly.

### Phase 9: Simplify EditSidePanel contract

Remove inline handlers from JSX. Pass engine methods directly. Remove duplicate revert-all.

Each phase is independently testable and committable. Phases 1-2 are safe mechanical refactors. Phase 3 is the critical correctness change. Phases 4-9 are incremental extractions.

---

## 11. Performance Analysis

### 11.1 Per-Cell Render Path (hottest path — called per visible cell per frame)

The virtualizer renders ~(visible rows + 12 overscan) × visible columns cells per scroll frame. Each cell calls two functions:

| Function | Current Cost | Proposed Cost | Change |
|----------|-------------|---------------|--------|
| `resolveDisplayedCellValue` | 1 string concat + 2 `Map.get` (optimistic + comparison) | 1 string concat + 2 `Map.get` (optimistic + unified store) | **Identical** |
| `resolveEditedCellMarker` | 1 string concat + 1 `Map.get` (markers) | 1 string concat + 1 `Map.get` (unified store) | **Identical** |

**Total per cell: 1 string concat + 3 `Map.get` → 1 string concat + 3 `Map.get`.** No regression.

**Micro-optimization**: The current `resolveEditedCellMarker` allocates a new `{ direct, propagated }` object on every call. The proposed design uses 3 frozen sentinel objects:

```js
const MARKER_DIRECT     = Object.freeze({ direct: true,  propagated: false });
const MARKER_PROPAGATED = Object.freeze({ direct: false, propagated: true });
const MARKER_BOTH       = Object.freeze({ direct: true,  propagated: true });
```

This eliminates one object allocation per cell per render frame. For a 50×20 visible grid = 1,000 cells × 60fps = 60,000 fewer GC-pressured objects/second during scroll. Small but free.

### 11.2 Edit Application Path (on each user edit)

Descendant walking is the bottleneck. V = visible row count (typically 50-200 with server-side).

| Operation | Current | Proposed | Change |
|-----------|---------|----------|--------|
| Descendant walk for markers | O(V) full scan | — | Eliminated (merged) |
| Descendant walk for comparison | O(V) full scan | — | Eliminated (merged) |
| Descendant walk for affected cells | — | O(V) full scan | New (replaces above two) |
| Optimistic ancestor rollup | O(depth × V) | O(depth × V) | Same |
| Map mutations | O(A) across 3 Maps | O(A) across 1 Map + 1 Map | **Fewer Map.set calls** |

**Net: one fewer O(V) pass per edit** (3 walks → 2 walks). For V=200: ~200 fewer iterations, saving ~0.05ms. Negligible individually but adds up with rapid edits.

### 11.3 Memory

| Resource | Current (per edited cell) | Proposed | Change |
|----------|--------------------------|----------|--------|
| Marker map entries | 1 entry in `editedCellMarkers` | — | Eliminated |
| Comparison entries | 1 in `originalValues` + 1 in `counts` | — | Eliminated |
| Unified store entries | — | 1 entry | New |
| Optimistic entries | 1 entry (while pending) | 1 entry (while pending) | Same |
| **Total Map entries** | **3-4 per cell** | **1-2 per cell** | **50-65% reduction** |

For a session with 500 edited cells: ~1,500-2,000 Map entries → ~500-1,000. Each entry is ~100 bytes (key string + value object). Saves ~50-100KB. Trivial in absolute terms but directionally correct.

### 11.4 React Re-render Batching

| Trigger | Current epochs bumped | Proposed epochs bumped |
|---------|----------------------|----------------------|
| Apply edit | `editedCellEpoch` + `comparisonCellEpoch` + `optimisticCellEpoch` (3 setState) | `storeEpoch` + `optimisticEpoch` (2 useReducer) |
| Undo/redo | `editedCellEpoch` + `comparisonCellEpoch` + `optimisticCellEpoch` (3) | `storeEpoch` + `optimisticEpoch` (2) |
| Revert | `editedCellEpoch` + `comparisonCellEpoch` (2) | `storeEpoch` (1) |

React batches setState calls within the same synchronous handler into one re-render, so the actual frame count is the same. But fewer state variables means fewer reconciliation steps in React's commit phase.

Using `useReducer(c => c + 1, 0)` instead of `useState` for epoch counters avoids the closure stale-capture issue that `useState` setters can have in complex callback chains.

### 11.5 Serialization

| Operation | Current | Proposed |
|-----------|---------|----------|
| Serialize | Iterate 2 Maps, build 2 arrays, 2 sorts, JSON.stringify | Iterate 1 Map, build 1 array, 1 sort, JSON.stringify |
| Payload size | `{ cells: [...], markers: [...] }` | `{ cells: [...] }` (markers merged into cells) |
| Deserialize | 2 passes to populate 2 Maps + marker reconciliation | 1 pass to populate 1 Map |

Serialization runs on every store change (debounced by the `useMemo` → `useEffect` chain). **Halving the iteration count is meaningful** for large edit sessions (500+ cells).

### 11.6 `buildAffectedCells` — Allocation Profile

The `treeWalker` generators yield raw `string` rowIds (no object allocation). The `buildAffectedCells` function only creates `{ rowId, colId }` objects when inserting into the result Maps. For a parent edit affecting 100 descendants:

- Current: allocates ~200 objects (100 per plan × 2 plans) + 2 Sets + 2 Arrays
- Proposed: allocates ~100 objects (100 in one Map) + 2 Maps

**50% fewer transient objects.**

### 11.7 Risk Assessment

| Concern | Assessment |
|---------|-----------|
| Unified store Map grows larger than any single current Map? | No — it has exactly the same number of entries as `editedCellMarkers` (the largest of the 3). Comparison-only entries (had originalValue but no marker) don't exist in practice. |
| `buildAffectedCells` is slower because it builds Maps instead of Sets? | Map vs Set for string keys: Map has ~5% overhead for small entries. For 100-200 entries, this is <0.01ms. Not measurable. |
| Extra indirection through the hook adds function call overhead? | The hot-path functions (`resolveMarker`, `resolveDisplayValue`) are `useCallback` wrapped. One extra closure layer vs. today's inline callbacks. V8 inlines these. No measurable cost. |
| `CellEditStore` class instance vs. plain Map? | The store is a plain object with a Map property, not a class with prototype chain. Method calls are direct property access. Same as calling a function on a module. |

### 11.8 Summary

The proposed architecture is **performance-neutral to slightly better** than the current system:

- **Hot path (per-cell render)**: Identical cost, plus one micro-optimization (sentinel markers).
- **Edit path**: One fewer O(V) pass, 50% fewer transient object allocations.
- **Memory**: 50-65% fewer Map entries.
- **Serialization**: Half the iteration count.
- **Re-renders**: Fewer epoch states to reconcile (2 vs 3).

No regressions identified. The architecture changes are structural (code organization) not algorithmic — the same Maps, same key format, same O(1) lookups.

---

## 12. Remaining Logic Gaps (Addressed Before Implementation)

### 12.1 Client-Only Mode (non-server-side)

When `serverSide = false`, there is no `inverseTransaction` from the server. The history-based revert cannot be used. Client-only mode uses a different path throughout:

| Operation | Server-side | Client-only |
|-----------|------------|-------------|
| Apply edit | Dispatch transaction → server returns inverse | `setProps({ cellUpdates })` directly |
| History entry | Has `inverseTransaction` + `redoTransaction` | Has only `affectedCells` + `originalValues` snapshot |
| Revert | Use `entry.inverseTransaction` | Use `entry.originalValues` → build explicit revert updates |
| Undo | Dispatch `entry.inverseTransaction` | `setProps({ cellUpdates: reverseUpdates })` built from snapshot |

For client-only history entries, store `originalValues: Map<cellKey, any>` (a snapshot at edit time from the store's `originalValue` fields). Revert builds `{rowId, colId, value: originalValues.get(k)}` updates for each key in `affectedCells`. This is the current behavior — only applied in the client-only path, not the server-side path where the server inverse is available.

The `HistoryEntry` type gets a `mode` field: `'server'` | `'client'`. Undo/revert check this before deciding which path to take.

### 12.2 Fallback When Server Returns No Capturable Inverse

The server sets `history.captureable = false` when:
- The aggregate edit identity couldn't be reconstructed
- The `inverseUpdate` or `redoUpdate` is null (line 3268 in `scalable_pivot_controller.py`)

When the server confirms an edit but returns `captureable: false`:
1. Still apply forward to the store (the edit happened)
2. Do NOT push to the undo stack
3. Show warning: "This edit cannot be undone."
4. The affected cells are still marked in the store (direct/propagated badges shown)
5. Revert of these cells falls back to Section 5.5's no-history path (send `originalValue` with `propagationStrategy: 'none'`)

This is clearly communicated to the user rather than silently failing.

### 12.3 Multi-Cell Revert Deduplication

`revertSelected(cells)` must not dispatch the same history entry's `inverseTransaction` twice. Example: Edit A propagated to both cell B and cell C. User selects B and C and clicks "Revert Selected."

Without deduplication:
- B → `collectDirectEditsFor` → [entryA]  
- C → `collectDirectEditsFor` → [entryA]  
- entryA dispatched twice → server applies inverse twice → data corruption

Correct approach:
```
1. For each selected cell, collect direct edit entries
2. Flatten + deduplicate by entry.id (use a Map<id, entry>)
3. Sort by createdAt descending (newest first)
4. Process each unique entry once
```

The deduplication must happen before any dispatch. This also means `revertSelected` correctly handles the case where selecting B alone would also implicitly cover C (same parent edit) — it reverts the whole edit, not just the visible selected cells.

### 12.4 Revert Is Destructive (Design Decision)

The plan removes reverted entries from the undo stack (`history.removeEntries`). This means **revert is not undoable via the undo button**. This is a deliberate trade-off:

**Pro**: Clean undo stack. After revert, the stack reflects the true state of the data. No confusion about what undo will do.

**Con**: User cannot "undo a revert" if they change their mind.

**Why not make it undoable?** Making revert undoable requires pushing the reverted entry's `redoTransaction` onto the undo stack as a new "re-apply" entry. This works for single reverts but breaks for `revertAll` (would need to push N entries in reverse order, then undo N times to restore all edits — confusing UX).

**Mitigation**: Disable "Revert" when undo is in progress (pending request). Show a confirmation dialog for `revertAll`. The user consciously chose to revert with full awareness.

### 12.5 Re-apply Uses `serverPropagation` for Validation

The `HistoryEntry.serverPropagation` field (Section 3.3) is used in `reapplyPropagation` to validate the re-apply is safe before dispatching:

```js
const prev = entry.serverPropagation;

// Block re-apply if previous edit had 'none' strategy and no actual rows were updated
if (prev.strategy === 'none' && prev.updatedRowCount === 0) {
  showNotification('Cannot re-apply: previous edit did not propagate to any rows.', 'warning');
  return;
}

// Block proportional re-apply if the current server value is 0 (would divide by zero)
if (method === 'proportional') {
  const currentAggregate = resolveCurrentCellValue(cell.rowId, cell.colId);
  if (Number(currentAggregate) === 0) {
    showNotification('Cannot use proportional propagation: current value is zero.', 'warning');
    return;
  }
}
```

The `serverPropagation.fromValue` is also used to construct the new forward edit with the correct `oldValue`, ensuring the server can compute the right delta even if the optimistic display value differs from the server's last confirmed value.
