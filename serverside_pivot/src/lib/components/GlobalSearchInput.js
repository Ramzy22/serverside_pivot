import React, { useEffect, useRef, useState } from 'react';

/** Quiet time after the last keystroke before the search becomes a filter (one data request). */
export const GLOBAL_SEARCH_COMMIT_DELAY_MS = 250;

/**
 * The global search box. Typing updates the box at once; the ``filters.global`` change, and with it
 * the server request, is committed once typing pauses, so a typed word is one request instead of
 * one per keystroke. Clearing the box commits immediately. A value changed elsewhere (saved view,
 * reset) replaces the draft.
 * @param {{ value: string, onCommit: (value: string) => void, style?: React.CSSProperties, placeholder?: string }} props
 * @returns {any}
 */
export function GlobalSearchInput({ value, onCommit, style, placeholder }) {
    const [draft, setDraft] = useState(value);
    const timerRef = useRef(/** @type {ReturnType<typeof setTimeout> | null} */ (null));
    const onCommitRef = useRef(onCommit);
    onCommitRef.current = onCommit;
    const committedRef = useRef(value);

    useEffect(() => {
        // Our own commit coming back must not overwrite what was typed since.
        if (value === committedRef.current) return;
        committedRef.current = value;
        if (timerRef.current !== null) clearTimeout(timerRef.current);
        timerRef.current = null;
        setDraft(value);
    }, [value]);

    useEffect(() => () => {
        if (timerRef.current !== null) clearTimeout(timerRef.current);
    }, []);

    /** @param {string} next */
    const schedule = (next) => {
        if (timerRef.current !== null) clearTimeout(timerRef.current);
        timerRef.current = null;
        if (!next) {
            committedRef.current = next;
            onCommitRef.current(next);
            return;
        }
        timerRef.current = setTimeout(() => {
            timerRef.current = null;
            committedRef.current = next;
            onCommitRef.current(next);
        }, GLOBAL_SEARCH_COMMIT_DELAY_MS);
    };

    return (
        <input
            data-testid="pivot-global-search"
            style={style}
            placeholder={placeholder}
            value={draft}
            onChange={e => {
                const next = e.target.value;
                setDraft(next);
                schedule(next);
            }}
        />
    );
}
