import React, { useEffect, useMemo, useRef } from 'react';
import ReactDOM from 'react-dom';
import { buildThemeCssVars } from '../utils/styles';

/** The focused element, looking through open shadow roots. @returns {Element | null} */
function deepActiveElement() {
    let active = typeof document === 'undefined' ? null : document.activeElement;
    while (active && active.shadowRoot && active.shadowRoot.activeElement) {
        active = active.shadowRoot.activeElement;
    }
    return active;
}

/**
 * Shared overlay dismissal hook: handles Escape (capture phase), outside clicks,
 * and clean focus restoration to the previously focused trigger element.
 *
 * @param {object} options
 * @param {boolean} options.open
 * @param {(() => void) | null | undefined} options.onClose
 * @param {Array<React.RefObject<any> | Element | null | undefined>} [options.refs]
 * @param {boolean} [options.restoreFocus]
 * @param {boolean} [options.enabled]
 */
export function useDismiss({
    open,
    onClose,
    refs = [],
    restoreFocus = true,
    enabled = true,
}) {
    const onCloseRef = useRef(onClose);
    onCloseRef.current = onClose;
    const refsRef = useRef(refs);
    refsRef.current = refs;
    const previousFocusRef = useRef(/** @type {HTMLElement | null} */ (null));

    useEffect(() => {
        if (!open || !enabled || typeof document === 'undefined') return undefined;

        const active = deepActiveElement();
        previousFocusRef.current = active && typeof (/** @type {any} */ (active)).focus === 'function'
            ? /** @type {HTMLElement} */ (active)
            : null;

        /** @param {Node | null} target @returns {boolean} */
        const isInsideRefs = (target) => {
            // composedPath() ends with document and window; only DOM nodes can be contained.
            if (!target || typeof (/** @type {any} */ (target)).nodeType !== 'number') return false;
            const list = refsRef.current || [];
            for (let i = 0; i < list.length; i += 1) {
                const entry = list[i];
                const el = entry && typeof entry === 'object' && 'current' in entry ? entry.current : entry;
                if (el && typeof el.contains === 'function' && el.contains(target)) {
                    return true;
                }
            }
            return false;
        };

        /** @param {KeyboardEvent} event */
        const handleKeyDown = (event) => {
            if (event.key !== 'Escape' || event.defaultPrevented) return;
            event.preventDefault();
            event.stopPropagation();
            if (typeof onCloseRef.current === 'function') {
                onCloseRef.current();
            }
        };

        /**
         * A document listener sees events from inside a shadow root (Streamlit) retargeted to the
         * shadow host, so ``event.target`` alone would call every inside click "outside".
         * @param {MouseEvent | PointerEvent} event
         */
        const handlePointerDown = (event) => {
            const target = /** @type {Node | null} */ (event.target);
            if (isInsideRefs(target)) return;
            const path = typeof event.composedPath === 'function' ? event.composedPath() : [];
            if (path.some(node => isInsideRefs(/** @type {Node | null} */ (node)))) return;
            if (typeof onCloseRef.current === 'function') {
                onCloseRef.current();
            }
        };

        document.addEventListener('keydown', handleKeyDown, true);
        document.addEventListener('mousedown', handlePointerDown, true);

        return () => {
            document.removeEventListener('keydown', handleKeyDown, true);
            document.removeEventListener('mousedown', handlePointerDown, true);
            if (restoreFocus && previousFocusRef.current) {
                const prev = previousFocusRef.current;
                const currentActive = deepActiveElement();
                const shouldRestore = !currentActive
                    || currentActive === document.body
                    || isInsideRefs(currentActive);
                if (shouldRestore && prev.isConnected) {
                    try {
                        prev.focus({ preventScroll: true });
                    } catch (_err) {
                        prev.focus();
                    }
                }
            }
            previousFocusRef.current = null;
        };
    }, [open, enabled, restoreFocus]);
}

/**
 * Resolves the safest portal container: ShadowRoot host container if mounted inside
 * a ShadowRoot (e.g. Streamlit/web component), otherwise `document.body`.
 * Returns `null` in SSR or pure-Node VDOM tests.
 *
 * @param {any} [anchorEl]
 * @returns {Element | DocumentFragment | null}
 */
export function resolveOverlayPortalTarget(anchorEl = null) {
    if (typeof document === 'undefined' || !document.body) return null;
    if (anchorEl && typeof anchorEl.getRootNode === 'function') {
        const root = anchorEl.getRootNode();
        if (typeof ShadowRoot !== 'undefined' && root instanceof ShadowRoot) {
            return root.firstElementChild || root;
        }
    }
    return document.body;
}

/**
 * Renders floating overlay content into a top-level portal while preserving
 * `data-pivot-workspace-root` selectors, `--sp-*` CSS tokens, colorScheme,
 * and `@media (prefers-reduced-motion: reduce)` rules.
 *
 * @param {{ children: React.ReactNode, theme?: any, anchorEl?: any, zIndex?: number }} props
 * @returns {any}
 */
export function OverlayPortal({ children, theme, anchorEl = null, zIndex = 10000 }) {
    const portalTarget = resolveOverlayPortalTarget(anchorEl);
    const themeVars = useMemo(() => (theme ? buildThemeCssVars(theme) : {}), [theme]);

    if (!portalTarget || typeof ReactDOM.createPortal !== 'function') {
        return <>{children}</>;
    }

    return ReactDOM.createPortal(
        <div
            data-pivot-workspace-root="true"
            data-pivot-overlay-portal="true"
            style={{
                ...themeVars,
                colorScheme: theme && theme.isDark ? 'dark' : 'light',
                fontFamily: (theme && theme.fontFamily) || undefined,
                position: 'fixed',
                top: 0,
                left: 0,
                width: 0,
                height: 0,
                zIndex,
                pointerEvents: 'none',
            }}
        >
            {children}
        </div>,
        portalTarget
    );
}

export default useDismiss;
