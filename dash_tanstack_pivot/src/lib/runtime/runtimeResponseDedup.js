/**
 * Pure response idempotency rules for the transport boundary.
 *
 * Responses may be replayed by Dash prop updates; only one state transition is
 * permitted for a request/status pair. Keeping this outside workspace
 * presentation makes the policy reusable by the eventual response domain.
 */
/**
 * @param {{kind?: string, status?: string, requestId?: string | number, request_id?: string | number} | null | undefined} response
 * @param {{requestId?: string | number, request_id?: string | number, columnId?: string, column_id?: string, search?: unknown, offset?: number, options?: unknown[]} | null | undefined} payload
 * @returns {string | null}
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

/**
 * @param {{current: Set<string>}} handledKeysRef
 * @param {string | null | undefined} responseKey
 * @returns {boolean}
 */
export function markRuntimeResponseHandled(handledKeysRef, responseKey) {
  if (!responseKey) return true;
  const handledKeys = handledKeysRef.current;
  if (handledKeys.has(responseKey)) return false;
  handledKeys.add(responseKey);
  if (handledKeys.size > 128) {
    const oldestKey = handledKeys.values().next().value;
    if (oldestKey !== undefined) handledKeys.delete(oldestKey);
  }
  return true;
}
