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

    const animationFrame = requestAnimationFrame(() => {
      const parent = parentRef.current;
      if (!parent) return;
      if (typeof savedScroll.top === 'number') parent.scrollTop = savedScroll.top;
      if (typeof savedScroll.left === 'number') parent.scrollLeft = savedScroll.left;
    });
    return () => cancelAnimationFrame(animationFrame);
  }, [parentRef, viewState]);
}
