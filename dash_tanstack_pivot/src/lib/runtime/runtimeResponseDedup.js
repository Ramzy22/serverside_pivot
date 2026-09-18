/**
 * Pure response idempotency rules for the transport boundary.
 *
 * Responses may be replayed by Dash prop updates; only one state transition is
 * permitted for a request/status pair. Keeping this outside workspace
 * presentation makes the policy reusable by the eventual response domain.
 */
export function buildRuntimeResponseProcessingKey(response, payload) {
  if (!response || typeof response !== 'object') return null;
  const kind = response.kind || 'unknown';
  const status = response.status || 'unknown';
  const requestId = response.requestId || response.request_id || payload && (payload.requestId || payload.request_id);
  if (requestId !== undefined && requestId !== null) {
    return `${kind}:${status}:${requestId}`;
  }
  if (kind === 'filter_options' && payload && typeof payload === 'object') {
    const columnId = payload.columnId || payload.column_id || '';
    const search = payload.search === undefined || payload.search === null ? '' : String(payload.search);
    const offset = payload.offset === undefined || payload.offset === null ? 0 : payload.offset;
    const optionCount = Array.isArray(payload.options) ? payload.options.length : 0;
    return `${kind}:${status}:${columnId}:${search}:${offset}:${optionCount}`;
  }
  return null;
}

export function markRuntimeResponseHandled(handledKeysRef, responseKey) {
  if (!responseKey) return true;
  const handledKeys = handledKeysRef.current;
  if (handledKeys.has(responseKey)) return false;
  handledKeys.add(responseKey);
  if (handledKeys.size > 128) {
    const oldestKey = handledKeys.values().next().value;
    handledKeys.delete(oldestKey);
  }
  return true;
}
