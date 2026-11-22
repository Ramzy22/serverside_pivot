/**
 * Ordered delivery of runtime responses (M3).
 *
 * Responses of every kind (data, filter options, chart, detail, export...) reach
 * the UI through one `runtimeResponse` value. Writing it as a single slot loses
 * responses: two arriving in the same tick are batched by React and only the
 * last is ever rendered, so e.g. filter options or a detail page vanish when a
 * viewport response lands with them. The queue delivers each response, in
 * arrival order, for one commit each; consumers keep their per-kind requestId
 * and state-epoch checks, which reject superseded responses.
 */

import { useCallback, useEffect, useState } from 'react';

export interface ResponseQueueState<T> {
    head: T | null; // the response consumers currently see
    pending: T[]; // delivered after head, in arrival order
}

export const EMPTY_RESPONSE_QUEUE: ResponseQueueState<never> = { head: null, pending: [] };

export function enqueueResponse<T>(state: ResponseQueueState<T>, response: T): ResponseQueueState<T> {
    if (response === null || response === undefined) return state;
    if (state.head === null || (state.pending.length === 0 && state.head === response)) {
        return state.head === response ? state : { head: response, pending: state.pending };
    }
    return { head: state.head, pending: [...state.pending, response] };
}

/** After consumers have seen `head`, move to the next pending response (no change when none is pending). */
export function advanceResponse<T>(state: ResponseQueueState<T>): ResponseQueueState<T> {
    if (state.pending.length === 0) return state;
    return { head: state.pending[0], pending: state.pending.slice(1) };
}

/** [response to render, enqueue]. Each queued response is rendered for at least one commit. */
export function useResponseQueue<T>(): [T | null, (response: T) => void] {
    const [state, setState] = useState<ResponseQueueState<T>>(() => EMPTY_RESPONSE_QUEUE as ResponseQueueState<T>);
    const enqueue = useCallback((response: T) => setState((s) => enqueueResponse(s, response)), []);
    useEffect(() => {
        // Effects of this commit (the consumers) have seen `head`; deliver the next one.
        if (state.pending.length > 0) setState(advanceResponse);
    }, [state]);
    return [state.head, enqueue];
}
