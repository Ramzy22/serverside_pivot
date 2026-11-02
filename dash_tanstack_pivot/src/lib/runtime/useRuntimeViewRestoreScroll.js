import { useEffect } from 'react';

const resolveRestoredState = viewState => {
  if (!viewState || typeof viewState !== 'object') return null;
  return viewState.state && typeof viewState.state === 'object' ? viewState.state : viewState;
};

/**
 * Applies a persisted workspace scroll position once the viewport owns a DOM
 * scroll container.  Setup restores serializable state; this hook restores the
 * DOM-only part of that state at the boundary that owns the DOM ref.
 */
export function useRuntimeViewRestoreScroll({ parentRef, viewState }) {
  useEffect(() => {
    const restored = resolveRestoredState(viewState);
    const savedScroll = restored?.scroll && typeof restored.scroll === 'object' ? restored.scroll : null;
    if (!savedScroll || !parentRef.current) return undefined;

    // The first frame can precede async server rows and column measurements.
    // Browser assignments clamp to the current extent, so retry until both
    // offsets have survived a complete frame. Native navigation cancels restore.
    let animationFrame;
    let cancelled = false;
    let matched = false;
    const cancel = () => {
      cancelled = true;
      cancelAnimationFrame(animationFrame);
    };
    const parent = parentRef.current;
    const events = ['wheel', 'pointerdown', 'keydown'];
    events.forEach(event => parent.addEventListener?.(event, cancel, { passive: true }));
    const apply = () => {
      if (cancelled || parentRef.current !== parent) return;
      const topMatches = typeof savedScroll.top !== 'number' || Math.abs(parent.scrollTop - savedScroll.top) <= 1;
      const leftMatches = typeof savedScroll.left !== 'number' || Math.abs(parent.scrollLeft - savedScroll.left) <= 1;
      if (matched && topMatches && leftMatches) return;
      matched = topMatches && leftMatches;
      if (typeof savedScroll.top === 'number') parent.scrollTop = savedScroll.top;
      if (typeof savedScroll.left === 'number') parent.scrollLeft = savedScroll.left;
      animationFrame = requestAnimationFrame(apply);
    };
    animationFrame = requestAnimationFrame(apply);
    return () => {
      cancel();
      events.forEach(event => parent.removeEventListener?.(event, cancel));
    };
  }, [parentRef, viewState]);
}
