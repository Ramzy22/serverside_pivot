import { useCallback } from 'react';

/**
 * Measures rendered grid chrome and schedules header-fit corrections.
 *
 * DOM measurement is kept separate from column ordering and pinning commands
 * so virtual-column changes have one isolated browser-facing dependency.
  @param {any} p0 @returns {any} */
export function useRuntimeColumnMeasurement({ autoSizeBounds, id, parentRef, table }) {
  /** @type {(...args: any[]) => any} */
  const getMeasurementRoot = useCallback(() => {
    if (parentRef && parentRef.current) return parentRef.current;
    if (typeof document === 'undefined') return null;
    return (typeof id === 'string' && id ? document.getElementById(id) : null) || document;
  }, [id, parentRef]);

  /** @type {(...args: any[]) => any} */
  const getRenderedHeaderWidthForColumn = useCallback(columnId => {
    if (typeof document === 'undefined' || typeof window === 'undefined' || !columnId) return null;
    /** @type {any} */
    const scopedRoot = getMeasurementRoot();
    if (!scopedRoot) return null;
    /** @type {any} */
    const headerElements = Array.from(scopedRoot.querySelectorAll('[role="columnheader"][data-header-column-id]'))
      .filter(element => element && element.dataset && element.dataset.headerColumnId === String(columnId));
    if (headerElements.length === 0) return null;
    /** @type {any} */
    let maxMeasuredWidth = null;
    headerElements.forEach(/** @param {any} [headerElement] @returns {any} */ headerElement => {
      /** @type {any} */
      const headerContent = headerElement.querySelector('[data-header-content="true"]');
      /** @type {any} */
      const headerStyle = window.getComputedStyle(headerElement);
      /** @type {any} */
      const horizontalChrome = (parseFloat(headerStyle.paddingLeft || '0') || 0) + (parseFloat(headerStyle.paddingRight || '0') || 0)
        + (parseFloat(headerStyle.borderLeftWidth || '0') || 0) + (parseFloat(headerStyle.borderRightWidth || '0') || 0);
      let naturalContentWidth = 0;
      if (headerContent) {
        /** @type {any} */
        const measurementClone = headerContent.cloneNode(true);
        measurementClone.style.position = 'absolute'; measurementClone.style.left = '-100000px'; measurementClone.style.top = '0';
        measurementClone.style.visibility = 'hidden'; measurementClone.style.pointerEvents = 'none'; measurementClone.style.width = 'max-content';
        measurementClone.style.minWidth = '0'; measurementClone.style.maxWidth = 'none'; measurementClone.style.overflow = 'visible';
        measurementClone.style.display = 'inline-flex'; measurementClone.style.flex = '0 0 auto';
        measurementClone.querySelectorAll('[data-header-text="true"]').forEach(/** @param {any} [textElement] @returns {any} */ textElement => {
          if (!textElement || !textElement.style) return;
          textElement.style.flex = '0 0 auto'; textElement.style.width = 'auto'; textElement.style.minWidth = '0';
          textElement.style.maxWidth = 'none'; textElement.style.overflow = 'visible'; textElement.style.textOverflow = 'clip'; textElement.style.display = 'inline-block';
        });
        // Keep the clone in the header's own style/inheritance scope. Streamlit
        // mounts the workspace in a ShadowRoot, which document.body cannot see.
        headerElement.appendChild(measurementClone);
        try {
          naturalContentWidth = Math.ceil(measurementClone.scrollWidth || measurementClone.getBoundingClientRect().width || 0);
        } finally {
          headerElement.removeChild(measurementClone);
        }
      }
      /** @type {any} */
      const candidateWidth = naturalContentWidth > 0 ? naturalContentWidth + horizontalChrome + autoSizeBounds.headerOverscan : null;
      if (!Number.isFinite(candidateWidth)) return;
      maxMeasuredWidth = maxMeasuredWidth === null ? candidateWidth : Math.max(maxMeasuredWidth, candidateWidth);
    });
    return maxMeasuredWidth;
  }, [autoSizeBounds.headerOverscan, getMeasurementRoot]);

  /** @type {(...args: any[]) => any} */
  const getRenderedCellWidthForColumn = useCallback(columnId => {
    if (typeof document === 'undefined' || typeof window === 'undefined' || !columnId) return null;
    /** @type {any} */
    const scopedRoot = getMeasurementRoot();
    if (!scopedRoot) return null;
    /** @type {any} */
    const cellElements = Array.from(scopedRoot.querySelectorAll('[role="gridcell"][data-colid]'))
      .filter(element => element && element.dataset && element.dataset.colid === String(columnId));
    if (cellElements.length === 0) return null;
    /** @type {any} */
    let maxMeasuredWidth = null;
    cellElements.forEach(/** @param {any} [cellElement] @returns {any} */ cellElement => {
      /** @type {any} */
      const cellStyle = window.getComputedStyle(cellElement);
      /** @type {any} */
      const horizontalChrome = (parseFloat(cellStyle.paddingLeft || '0') || 0) + (parseFloat(cellStyle.paddingRight || '0') || 0)
        + (parseFloat(cellStyle.borderLeftWidth || '0') || 0) + (parseFloat(cellStyle.borderRightWidth || '0') || 0);
      /** @type {any} */
      const contentContainer = Array.from(cellElement.children || []).find(childElement => childElement && childElement.tagName === 'SPAN') || cellElement.querySelector('span');
      let contentWidth = 0;
      if (contentContainer) {
        /** @type {any} */
        const contentStyle = window.getComputedStyle(contentContainer);
        /** @type {any} */
        const gap = parseFloat(contentStyle.columnGap || contentStyle.gap || '0') || 0;
        /** @type {any} */
        const horizontalPadding = (parseFloat(contentStyle.paddingLeft || '0') || 0) + (parseFloat(contentStyle.paddingRight || '0') || 0);
        /** @type {any} */
        const childElements = Array.from(contentContainer.children || []);
        if (childElements.length > 0) {
          /** @type {any} */
          const childWidths = childElements.reduce(/** @param {any} [sum] @param {any} [childElement] @returns {any} */ (sum, childElement) => sum + Math.ceil(childElement.scrollWidth || childElement.getBoundingClientRect().width || 0), 0);
          contentWidth = childWidths + horizontalPadding + gap * Math.max(0, childElements.length - 1);
        }
        if (contentWidth <= 0) contentWidth = Math.ceil(contentContainer.scrollWidth || contentContainer.getBoundingClientRect().width || 0);
      }
      /** @type {any} */
      const candidateWidth = contentWidth > 0 ? Math.ceil(contentWidth + horizontalChrome + autoSizeBounds.cellOverscan) : null;
      if (!Number.isFinite(candidateWidth)) return;
      maxMeasuredWidth = maxMeasuredWidth === null ? candidateWidth : Math.max(maxMeasuredWidth, candidateWidth);
    });
    return maxMeasuredWidth;
  }, [autoSizeBounds.cellOverscan, getMeasurementRoot]);

  /** @type {(...args: any[]) => any} */
  const queueHeaderFitValidation = useCallback((columnId, measuredWidth) => {
    if (typeof window === 'undefined' || !columnId || !table || typeof table.setColumnSizing !== 'function') return;
    /** @type {any} */
    const requestFrame = window.requestAnimationFrame || (/** @param {any} [callback] @returns {any} */ callback => window.setTimeout(callback, 0));
    requestFrame(() => {
      requestFrame(() => {
        /** @type {any} */
        const renderedHeaderWidth = getRenderedHeaderWidthForColumn(columnId);
        if (!Number.isFinite(renderedHeaderWidth)) return;
        /** @type {any} */
        const currentColumn = typeof table.getColumn === 'function' ? table.getColumn(columnId) : null;
        /** @type {any} */
        const currentWidth = currentColumn && typeof currentColumn.getSize === 'function' ? currentColumn.getSize() : measuredWidth;
        /** @type {any} */
        const targetWidth = Math.min(autoSizeBounds.maxWidth, Math.max(autoSizeBounds.minWidth, Math.ceil(renderedHeaderWidth)));
        if (targetWidth > Math.max(measuredWidth, currentWidth) + 1) {
          table.setColumnSizing(/** @param {any} [previous] @returns {any} */ previous => ({ ...previous, [columnId]: targetWidth }));
        }
      });
    });
  }, [autoSizeBounds.maxWidth, autoSizeBounds.minWidth, getRenderedHeaderWidthForColumn, table]);

  return { getRenderedCellWidthForColumn, getRenderedHeaderWidthForColumn, queueHeaderFitValidation };
}
