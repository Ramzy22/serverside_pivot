import { useEffect } from 'react';

/** @param {any} [viewState] @returns {any} */
const resolveRestoredState = viewState => {
  if (!viewState || typeof viewState !== 'object') return null;
  return viewState.state && typeof viewState.state === 'object' ? viewState.state : viewState;
};

/**
 * Applies a persisted workspace scroll position once the viewport owns a DOM
 * scroll container.  Setup restores serializable state; this hook restores the
 * DOM-only part of that state at the boundary that owns the DOM ref.
  @param {any} p0 @returns {any} */
export function useRuntimeViewRestoreScroll({ parentRef, viewState }) {
  useEffect(() => {
    /** @type {any} */
    const restored = resolveRestoredState(viewState);
    /** @type {any} */
    const savedScroll = restored?.scroll && typeof restored.scroll === 'object' ? restored.scroll : null;
    if (!savedScroll || !parentRef.current) return undefined;

    /** @type {any} */
    const top = Number.isFinite(savedScroll.top) ? Math.max(0, savedScroll.top) : null;
    /** @type {any} */
    const left = Number.isFinite(savedScroll.left) ? Math.max(0, savedScroll.left) : null;
    if (top === null && left === null) return undefined;

    // Browser assignments clamp before async rows/column measurements arrive.
    // Try once per extent change, then stay idle while an offset is unreachable.
    // Observers allow even slow server responses without an animation-frame loop.
    const parent = parentRef.current;
    /** @type {any} */
    let animationFrame;
    let stopped = false;
    /** @type {any} */
    let mutationObserver;
    /** @type {any} */
    let resizeObserver;
    /** @type {any[]} */
    const events = ['wheel', 'pointerdown', 'keydown'];
    /** @returns {any} */
    const stop = () => {
      stopped = true;
      cancelAnimationFrame(animationFrame);
      mutationObserver?.disconnect();
      resizeObserver?.disconnect();
      events.forEach(event => parent.removeEventListener(event, stop));
    };
    /** @returns {any} */
    const matches = () => (top === null || Math.abs(parent.scrollTop - top) <= 1)
      && (left === null || Math.abs(parent.scrollLeft - left) <= 1);
    /** @returns {any} */
    const apply = () => {
      if (stopped || parentRef.current !== parent) { stop(); return; }
      if (top !== null) parent.scrollTop = top;
      if (left !== null) parent.scrollLeft = left;
      animationFrame = requestAnimationFrame(() => {
        if (stopped || parentRef.current !== parent || matches()) stop();
        // Otherwise remain dormant until the measured extent actually changes.
      });
    };
    /** @returns {any} */
    const extent = () => [parent.scrollHeight, parent.scrollWidth, parent.clientHeight, parent.clientWidth].join(':');
    /** @type {any} */
    let previousExtent = extent();
    /** @returns {any} */
    const onExtentChange = () => {
      if (stopped) return;
      /** @type {any} */
      const nextExtent = extent();
      if (nextExtent === previousExtent) return;
      previousExtent = nextExtent;
      cancelAnimationFrame(animationFrame);
      animationFrame = requestAnimationFrame(apply);
    };
    mutationObserver = new MutationObserver(onExtentChange);
    mutationObserver.observe(parent, { childList: true, subtree: true, attributes: true });
    resizeObserver = new ResizeObserver(onExtentChange);
    resizeObserver.observe(parent);
    events.forEach(event => parent.addEventListener(event, stop, { passive: true }));
    animationFrame = requestAnimationFrame(apply);
    return stop;
  }, [parentRef, viewState]);
}
