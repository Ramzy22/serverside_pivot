import { useLayoutEffect, useRef, useState } from 'react';

// Fixed coordinates belong to the trigger's document (including iframe hosts).
/** @param {any} [anchor] @param {any} [menu] @param {any} [viewport] @returns {any} */
export function placeChartExportMenu(anchor, menu, viewport) {
    const margin = 8;
    const gap = 6;
    /** @type {any} */
    const width = Math.min(menu.width, Math.max(0, viewport.width - margin * 2));
    /** @type {any} */
    const below = Math.max(0, viewport.height - margin - anchor.bottom - gap);
    /** @type {any} */
    const above = Math.max(0, anchor.top - gap - margin);
    /** @type {any} */
    const openAbove = menu.height > below && above > below;
    /** @type {any} */
    const maxHeight = openAbove ? above : below;
    /** @type {any} */
    const height = Math.min(menu.height, maxHeight);
    return {
        left: Math.max(margin, Math.min(anchor.right - width, viewport.width - margin - width)),
        top: openAbove ? anchor.top - gap - height : anchor.bottom + gap,
        maxWidth: Math.max(0, viewport.width - margin * 2),
        maxHeight,
    };
}

/** @param {any} [open] @param {any} [setOpen] @returns {any} */
export function useChartExportMenu(open, setOpen) {
    /** @type {any} */
    const anchorRef = useRef(null);
    /** @type {any} */
    const menuRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [position, setPosition] = useState(null);
    const ownerDocument = anchorRef.current?.ownerDocument;
    // Fullscreen excludes body siblings from rendering. Use its unclipped root
    // while fullscreen, otherwise leave all chart/ShadowRoot clipping ancestors.
    /** @type {any} */
    const portalTarget = ownerDocument?.fullscreenElement || ownerDocument?.body;

    useLayoutEffect(() => {
        if (!open || !anchorRef.current || !menuRef.current) return undefined;
        const anchor = anchorRef.current;
        const menu = menuRef.current;
        const doc = anchor.ownerDocument;
        const view = doc.defaultView;
        /** @returns {any} */
        const updatePosition = () => setPosition(placeChartExportMenu(
            anchor.getBoundingClientRect(),
            { width: menu.offsetWidth, height: menu.scrollHeight + 2 },
            { width: doc.documentElement.clientWidth, height: doc.documentElement.clientHeight },
        ));
        /** @returns {any} */
        const enabledItems = () => Array.from(menu.querySelectorAll('[role="menuitem"]:not(:disabled)'));
        /** @param {any} [restoreFocus] @returns {any} */
        const close = (restoreFocus = false) => {
            setOpen(false);
            setPosition(null);
            if (restoreFocus) anchor.focus();
        };
        /** @param {any} [event] @returns {any} */
        const outside = event => {
            /** @type {any} */
            const eventPath = event.composedPath?.() || [];
            if (![anchor, menu].some(node => eventPath.includes(node) || node.contains(event.target))) close();
        };
        /** @param {any} [event] @returns {any} */
        const keydown = event => {
            if (event.key === 'Escape') {
                event.preventDefault();
                event.stopPropagation();
                close(true);
            } else if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
                if (!menu.contains(event.target) && !event.composedPath?.().includes(anchor)) return;
                event.preventDefault();
                /** @type {any} */
                const items = enabledItems();
                /** @type {any} */
                const index = items.indexOf(doc.activeElement);
                /** @type {any} */
                const next = event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1
                    : (index + (event.key === 'ArrowUp' ? -1 : 1) + items.length) % items.length;
                items[next]?.focus();
            } else if (event.key === 'Tab') close(true);
        };
        updatePosition();
        doc.addEventListener('mousedown', outside, true);
        doc.addEventListener('keydown', keydown, true);
        doc.addEventListener('scroll', updatePosition, true);
        view.addEventListener('resize', updatePosition);
        /** @type {any} */
        const observer = typeof view.ResizeObserver === 'function' ? new view.ResizeObserver(updatePosition) : null;
        observer?.observe(anchor);
        observer?.observe(menu);
        return () => {
            doc.removeEventListener('mousedown', outside, true);
            doc.removeEventListener('keydown', keydown, true);
            doc.removeEventListener('scroll', updatePosition, true);
            view.removeEventListener('resize', updatePosition);
            observer?.disconnect();
        };
    }, [open, portalTarget, setOpen]);

    // The first commit is hidden until measured. Focus after its positioned,
    // visible commit; browsers cannot focus the initially hidden menu items.
    useLayoutEffect(() => {
        if (open && position) menuRef.current?.querySelector('[role="menuitem"]:not(:disabled)')?.focus();
    }, [open, Boolean(position)]);

    return { anchorRef, menuRef, position, portalTarget };
}
