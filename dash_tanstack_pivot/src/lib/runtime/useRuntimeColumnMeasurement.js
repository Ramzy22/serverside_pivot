import { useCallback } from 'react';

/**
 * Measures rendered grid chrome and schedules header-fit corrections.
 *
 * DOM measurement is kept separate from column ordering and pinning commands
 * so virtual-column changes have one isolated browser-facing dependency.
 */
export function useRuntimeColumnMeasurement({ autoSizeBounds, id, table }) {
  const getRenderedHeaderWidthForColumn = useCallback(columnId => {
    if (typeof document === 'undefined' || typeof window === 'undefined' || !columnId) return null;
    const scopedRoot = typeof id === 'string' && id ? document.getElementById(id) : null;
    const headerElements = Array.from((scopedRoot || document).querySelectorAll('[role="columnheader"][data-header-column-id]'))
      .filter(element => element && element.dataset && element.dataset.headerColumnId === String(columnId));
    if (headerElements.length === 0) return null;
    let maxMeasuredWidth = null;
    headerElements.forEach(headerElement => {
      const headerContent = headerElement.querySelector('[data-header-content="true"]');
      const headerStyle = window.getComputedStyle(headerElement);
      const horizontalChrome = (parseFloat(headerStyle.paddingLeft || '0') || 0) + (parseFloat(headerStyle.paddingRight || '0') || 0)
        + (parseFloat(headerStyle.borderLeftWidth || '0') || 0) + (parseFloat(headerStyle.borderRightWidth || '0') || 0);
      let naturalContentWidth = 0;
      if (headerContent && document.body) {
        const measurementClone = headerContent.cloneNode(true);
        measurementClone.style.position = 'absolute'; measurementClone.style.left = '-100000px'; measurementClone.style.top = '0';
        measurementClone.style.visibility = 'hidden'; measurementClone.style.pointerEvents = 'none'; measurementClone.style.width = 'max-content';
        measurementClone.style.minWidth = '0'; measurementClone.style.maxWidth = 'none'; measurementClone.style.overflow = 'visible';
        measurementClone.style.display = 'inline-flex'; measurementClone.style.flex = '0 0 auto';
        measurementClone.querySelectorAll('[data-header-text="true"]').forEach(textElement => {
          if (!textElement || !textElement.style) return;
          textElement.style.flex = '0 0 auto'; textElement.style.width = 'auto'; textElement.style.minWidth = '0';
          textElement.style.maxWidth = 'none'; textElement.style.overflow = 'visible'; textElement.style.textOverflow = 'clip'; textElement.style.display = 'inline-block';
        });
        document.body.appendChild(measurementClone);
        naturalContentWidth = Math.ceil(measurementClone.scrollWidth || measurementClone.getBoundingClientRect().width || 0);
        document.body.removeChild(measurementClone);
      }
      const candidateWidth = naturalContentWidth > 0 ? naturalContentWidth + horizontalChrome + autoSizeBounds.headerOverscan : null;
      if (!Number.isFinite(candidateWidth)) return;
      maxMeasuredWidth = maxMeasuredWidth === null ? candidateWidth : Math.max(maxMeasuredWidth, candidateWidth);
    });
    return maxMeasuredWidth;
  }, [autoSizeBounds.headerOverscan, id]);

  const getRenderedCellWidthForColumn = useCallback(columnId => {
    if (typeof document === 'undefined' || typeof window === 'undefined' || !columnId) return null;
    const scopedRoot = typeof id === 'string' && id ? document.getElementById(id) : null;
    const cellElements = Array.from((scopedRoot || document).querySelectorAll('[role="gridcell"][data-colid]'))
      .filter(element => element && element.dataset && element.dataset.colid === String(columnId));
    if (cellElements.length === 0) return null;
    let maxMeasuredWidth = null;
    cellElements.forEach(cellElement => {
      const cellStyle = window.getComputedStyle(cellElement);
      const horizontalChrome = (parseFloat(cellStyle.paddingLeft || '0') || 0) + (parseFloat(cellStyle.paddingRight || '0') || 0)
        + (parseFloat(cellStyle.borderLeftWidth || '0') || 0) + (parseFloat(cellStyle.borderRightWidth || '0') || 0);
      const contentContainer = Array.from(cellElement.children || []).find(childElement => childElement && childElement.tagName === 'SPAN') || cellElement.querySelector('span');
      let contentWidth = 0;
      if (contentContainer) {
        const contentStyle = window.getComputedStyle(contentContainer);
        const gap = parseFloat(contentStyle.columnGap || contentStyle.gap || '0') || 0;
        const horizontalPadding = (parseFloat(contentStyle.paddingLeft || '0') || 0) + (parseFloat(contentStyle.paddingRight || '0') || 0);
        const childElements = Array.from(contentContainer.children || []);
        if (childElements.length > 0) {
          const childWidths = childElements.reduce((sum, childElement) => sum + Math.ceil(childElement.scrollWidth || childElement.getBoundingClientRect().width || 0), 0);
          contentWidth = childWidths + horizontalPadding + gap * Math.max(0, childElements.length - 1);
        }
        if (contentWidth <= 0) contentWidth = Math.ceil(contentContainer.scrollWidth || contentContainer.getBoundingClientRect().width || 0);
      }
      const candidateWidth = contentWidth > 0 ? Math.ceil(contentWidth + horizontalChrome + autoSizeBounds.cellOverscan) : null;
      if (!Number.isFinite(candidateWidth)) return;
      maxMeasuredWidth = maxMeasuredWidth === null ? candidateWidth : Math.max(maxMeasuredWidth, candidateWidth);
    });
    return maxMeasuredWidth;
  }, [autoSizeBounds.cellOverscan, id]);

  const queueHeaderFitValidation = useCallback((columnId, measuredWidth) => {
    if (typeof window === 'undefined' || !columnId || !table || typeof table.setColumnSizing !== 'function') return;
    const requestFrame = window.requestAnimationFrame || (callback => window.setTimeout(callback, 0));
    requestFrame(() => {
      requestFrame(() => {
        const renderedHeaderWidth = getRenderedHeaderWidthForColumn(columnId);
        if (!Number.isFinite(renderedHeaderWidth)) return;
        const currentColumn = typeof table.getColumn === 'function' ? table.getColumn(columnId) : null;
        const currentWidth = currentColumn && typeof currentColumn.getSize === 'function' ? currentColumn.getSize() : measuredWidth;
        const targetWidth = Math.min(autoSizeBounds.maxWidth, Math.max(autoSizeBounds.minWidth, Math.ceil(renderedHeaderWidth)));
        if (targetWidth > Math.max(measuredWidth, currentWidth) + 1) {
          table.setColumnSizing(previous => ({ ...previous, [columnId]: targetWidth }));
        }
      });
    });
  }, [autoSizeBounds.maxWidth, autoSizeBounds.minWidth, getRenderedHeaderWidthForColumn, table]);

  return { getRenderedCellWidthForColumn, getRenderedHeaderWidthForColumn, queueHeaderFitValidation };
}
