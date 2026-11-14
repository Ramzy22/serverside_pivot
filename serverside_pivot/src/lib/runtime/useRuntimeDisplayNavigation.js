import { useCallback } from 'react';

/**
 * Owns navigation through the rendered, virtualized display axes.
 *
 * These commands need both virtualizers and the table scroll container.  They
 * therefore belong to the viewport stage, where those refs are created, rather
 * than to setup, which only owns the display-row index contract.
  @param {any} p0 @returns {any} */
export function useRuntimeDisplayNavigation({
  activeRowVirtualizerRef,
  columnVirtualizerRef,
  parentRef,
  pinnedColumnMetaRef,
  pinnedDisplayMetaRef
}) {
  /** @type {(...args: any[]) => any} */
  const scrollToDisplayRow = useCallback(displayIndex => {
    const currentRowVirtualizer = activeRowVirtualizerRef.current;
    if (!currentRowVirtualizer || !currentRowVirtualizer.scrollToIndex) return;
    const {
      topCount,
      centerCount
    } = pinnedDisplayMetaRef.current;
    if (displayIndex < topCount) {
      if (parentRef.current) parentRef.current.scrollTop = 0;
      return;
    }
    if (displayIndex >= topCount + centerCount) {
      currentRowVirtualizer.scrollToIndex(Math.max(centerCount - 1, 0));
      return;
    }
    currentRowVirtualizer.scrollToIndex(Math.max(displayIndex - topCount, 0));
  }, [activeRowVirtualizerRef, parentRef, pinnedDisplayMetaRef]);

  /** @type {(...args: any[]) => any} */
  const scrollToDisplayColumn = useCallback(displayIndex => {
    const currentColumnVirtualizer = columnVirtualizerRef.current;
    if (!currentColumnVirtualizer || !currentColumnVirtualizer.scrollToIndex) return;
    const {
      leftCount,
      centerCount
    } = pinnedColumnMetaRef.current;
    if (displayIndex < leftCount) {
      if (parentRef.current) parentRef.current.scrollLeft = 0;
      return;
    }
    if (displayIndex >= leftCount + centerCount) {
      currentColumnVirtualizer.scrollToIndex(Math.max(centerCount - 1, 0));
      return;
    }
    currentColumnVirtualizer.scrollToIndex(Math.max(displayIndex - leftCount, 0));
  }, [columnVirtualizerRef, parentRef, pinnedColumnMetaRef]);

  return {
    scrollToDisplayColumn,
    scrollToDisplayRow
  };
}
