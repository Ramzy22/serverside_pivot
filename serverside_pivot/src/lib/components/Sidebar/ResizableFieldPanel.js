import React from 'react';
import { getFieldPanelLimits, sanitizeFieldPanelSizeEntry } from '../../utils/fieldPanelLayout';

/**
 * A field-list panel with drag handles for width, height and both; the width
 * handle widens the sidebar when the panel outgrows it.
 * @param {any} p0 @returns {any}
 */
export function ResizableFieldPanel({
    panelId,
    panelLabel,
    size,
    onSizeChange,
    getSidebarWidth = null,
    onSidebarWidthChange = null,
    theme,
    outerStyle = {},
    contentStyle = {},
    children,
}) {
    /** @type {any} */ const panelRef = React.useRef(null);
    /** @type {any} */ const cleanupRef = React.useRef(null);
    /** @type {any} */ const rafIdRef = React.useRef(null);
    /** @type {any} */ const latestDragRef = React.useRef(null);
    /** @type {any} */ const pendingSidebarWidthRef = React.useRef(null);
    /** @type {any} */ const baselineSidebarWidthRef = React.useRef(null);
    /** @type {any} */ const baselineChromeWidthRef = React.useRef(null);
    /** @type {[any, (...args: any[]) => any]} */ const [dragSize, setDragSize] = React.useState(null);
    /** @type {[any, (...args: any[]) => any]} */ const [activeDirection, setActiveDirection] = React.useState(null);
    /** @type {any} */ const limits = React.useMemo(() => getFieldPanelLimits(panelId), [panelId]);
    /** @type {any} */ const resolvedSize = React.useMemo(
        () => sanitizeFieldPanelSizeEntry(panelId, size),
        [panelId, size]
    );
    /** @type {any} */ const resolvedSizeRef = React.useRef(resolvedSize);
    resolvedSizeRef.current = resolvedSize;
    /** @type {any} */ const displayedSize = React.useMemo(() => {
        if (!dragSize) return resolvedSize;
        return sanitizeFieldPanelSizeEntry(panelId, {
            width: dragSize.width !== undefined ? dragSize.width : resolvedSize.width,
            height: dragSize.height !== undefined ? dragSize.height : resolvedSize.height,
        });
    }, [dragSize, panelId, resolvedSize]);

    /** @type {(...args: any[]) => any} */ const stopActiveResize = React.useCallback((commit = true) => {
        if (cleanupRef.current) {
            const cleanup = cleanupRef.current;
            cleanupRef.current = null;
            cleanup(commit);
        }
    }, []);

    React.useEffect(() => (
        () => stopActiveResize(false)
    ), [stopActiveResize]);

    /** @type {(...args: any[]) => any} */ const startResize = React.useCallback((event, direction) => {
        if (!panelRef.current || typeof onSizeChange !== 'function') return;
        event.preventDefault();
        event.stopPropagation();
        stopActiveResize(false);

        /** @type {any} */ const rect = panelRef.current.getBoundingClientRect();
        /** @type {any} */ const parentRect = panelRef.current.parentElement
            ? panelRef.current.parentElement.getBoundingClientRect()
            : rect;
        /** @type {any} */ const measuredSidebarWidth = typeof getSidebarWidth === 'function' ? Number(getSidebarWidth()) : NaN;
        /** @type {any} */ const startSidebarWidth = Number.isFinite(measuredSidebarWidth)
            ? Math.max(200, Math.min(600, Math.round(measuredSidebarWidth)))
            : null;
        /** @type {any} */ const chromeWidth = startSidebarWidth !== null
            ? Math.max(0, Math.round(startSidebarWidth - parentRect.width))
            : 0;
        /** @type {any} */ const canExpandSidebar = typeof onSidebarWidthChange === 'function' && startSidebarWidth !== null;
        if (startSidebarWidth !== null && (resolvedSizeRef.current.width === null || baselineSidebarWidthRef.current === null)) {
            baselineSidebarWidthRef.current = startSidebarWidth;
            baselineChromeWidthRef.current = chromeWidth;
        }
        /** @type {any} */ const baseSidebarWidth = startSidebarWidth !== null
            ? (baselineSidebarWidthRef.current !== null ? Math.min(startSidebarWidth, baselineSidebarWidthRef.current) : startSidebarWidth)
            : null;
        /** @type {any} */ const effectiveChromeWidth = baselineChromeWidthRef.current !== null
            ? baselineChromeWidthRef.current
            : chromeWidth;
        /** @type {any} */ const maxAllowedWidth = canExpandSidebar
            ? Math.max(limits.minWidth, Math.min(limits.maxWidth, 600 - effectiveChromeWidth))
            : Math.max(limits.minWidth, Math.min(limits.maxWidth, Math.floor(parentRect.width)));
        /** @type {Record<string, any>} */ const startState = {
            startX: event.clientX,
            startY: event.clientY,
            startWidth: rect.width,
            startHeight: rect.height,
            startParentWidth: Math.floor(parentRect.width),
            startSidebarWidth,
            baseSidebarWidth,
            chromeWidth: effectiveChromeWidth,
            canExpandSidebar,
            maxWidth: maxAllowedWidth,
            initialWidthProp: resolvedSizeRef.current.width,
        };
        const previousCursor = document.body.style.cursor;
        const previousUserSelect = document.body.style.userSelect;
        document.body.style.cursor = direction === 'both'
            ? 'nwse-resize'
            : (direction === 'width' ? 'ew-resize' : 'ns-resize');
        document.body.style.userSelect = 'none';
        latestDragRef.current = null;
        pendingSidebarWidthRef.current = null;
        setActiveDirection(direction);

        const flushFrame = () => {
            rafIdRef.current = null;
            if (latestDragRef.current) {
                setDragSize(latestDragRef.current);
            }
            if (pendingSidebarWidthRef.current !== null && typeof onSidebarWidthChange === 'function') {
                onSidebarWidthChange(pendingSidebarWidthRef.current);
                pendingSidebarWidthRef.current = null;
            }
        };

        /** @param {any} [moveEvent] @returns {any} */ const onMouseMove = (moveEvent) => {
            /** @type {any} */ const deltaX = moveEvent.clientX - startState.startX;
            /** @type {any} */ const deltaY = moveEvent.clientY - startState.startY;
            /** @type {Record<string, any>} */ const nextPartial = {};
            if (direction === 'width' || (direction === 'both' && (startState.initialWidthProp !== null || Math.abs(deltaX) >= 3))) {
                const nextWidth = Math.max(limits.minWidth, Math.min(startState.maxWidth, Math.round(startState.startWidth + deltaX)));
                nextPartial.width = nextWidth;
                if (panelRef.current) {
                    panelRef.current.style.width = `${nextWidth}px`;
                }
                if (startState.canExpandSidebar) {
                    const targetFloorWidth = startState.baseSidebarWidth !== null
                        ? startState.baseSidebarWidth
                        : startState.startSidebarWidth;
                    const desiredSidebarWidth = Math.max(
                        targetFloorWidth,
                        Math.min(600, nextWidth + startState.chromeWidth)
                    );
                    pendingSidebarWidthRef.current = desiredSidebarWidth;
                }
            }
            if (direction === 'height' || direction === 'both') {
                const nextHeight = Math.max(limits.minHeight, Math.min(limits.maxHeight, Math.round(startState.startHeight + deltaY)));
                nextPartial.height = nextHeight;
                if (panelRef.current) {
                    panelRef.current.style.height = `${nextHeight}px`;
                }
            }
            if (Object.keys(nextPartial).length === 0) return;
            latestDragRef.current = nextPartial;
            if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
                if (rafIdRef.current === null) {
                    rafIdRef.current = window.requestAnimationFrame(flushFrame);
                }
            } else {
                flushFrame();
            }
        };

        /** @param {boolean} [commit] @returns {any} */ const cleanup = (commit = true) => {
            if (rafIdRef.current !== null && typeof window !== 'undefined' && typeof window.cancelAnimationFrame === 'function') {
                window.cancelAnimationFrame(rafIdRef.current);
                rafIdRef.current = null;
            }
            document.body.style.cursor = previousCursor;
            document.body.style.userSelect = previousUserSelect;
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);
            window.removeEventListener('blur', onMouseUp);
            const committedSize = latestDragRef.current;
            const committedSidebarWidth = pendingSidebarWidthRef.current;
            latestDragRef.current = null;
            pendingSidebarWidthRef.current = null;
            setActiveDirection(null);
            setDragSize(null);
            if (commit) {
                if (committedSidebarWidth !== null && typeof onSidebarWidthChange === 'function') {
                    onSidebarWidthChange(committedSidebarWidth);
                }
                if (committedSize) {
                    onSizeChange(panelId, committedSize);
                }
            } else if (panelRef.current) {
                panelRef.current.style.width = resolvedSizeRef.current.width === null
                    ? '100%'
                    : `${resolvedSizeRef.current.width}px`;
                panelRef.current.style.height = `${resolvedSizeRef.current.height}px`;
            }
        };

        /** @returns {any} */ const onMouseUp = () => stopActiveResize(true);
        cleanupRef.current = cleanup;
        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);
        window.addEventListener('blur', onMouseUp);
    }, [
        getSidebarWidth,
        limits.maxHeight,
        limits.maxWidth,
        limits.minHeight,
        limits.minWidth,
        onSidebarWidthChange,
        onSizeChange,
        panelId,
        stopActiveResize,
    ]);

    /** @type {(...args: any[]) => any} */ const handleResetSize = React.useCallback((direction, event) => {
        if (event) {
            event.preventDefault();
            event.stopPropagation();
        }
        stopActiveResize(false);
        if (typeof onSizeChange !== 'function') return;
        if (direction === 'width') {
            onSizeChange(panelId, { width: null });
            if (baselineSidebarWidthRef.current !== null && typeof onSidebarWidthChange === 'function') {
                onSidebarWidthChange(baselineSidebarWidthRef.current);
                baselineSidebarWidthRef.current = null;
                baselineChromeWidthRef.current = null;
            }
        } else if (direction === 'height') {
            onSizeChange(panelId, { height: null });
        } else {
            onSizeChange(panelId, { width: null, height: null });
            if (baselineSidebarWidthRef.current !== null && typeof onSidebarWidthChange === 'function') {
                onSidebarWidthChange(baselineSidebarWidthRef.current);
                baselineSidebarWidthRef.current = null;
                baselineChromeWidthRef.current = null;
            }
        }
    }, [onSidebarWidthChange, onSizeChange, panelId, stopActiveResize]);

    const handleColor = `${theme.primary}55`;

    return (
        <div
            ref={panelRef}
            style={{
                position: 'relative',
                flexShrink: 0,
                width: displayedSize.width === null ? '100%' : `${displayedSize.width}px`,
                maxWidth: '100%',
                height: `${displayedSize.height}px`,
                minHeight: `${limits.minHeight}px`,
                maxHeight: `${limits.maxHeight}px`,
                boxSizing: 'border-box',
                overflow: 'hidden',
                ...outerStyle,
            }}
        >
            <div
                style={{
                    width: '100%',
                    height: '100%',
                    overflow: 'auto',
                    boxSizing: 'border-box',
                    ...contentStyle,
                }}
            >
                {children}
            </div>
            <div
                className="sp-resize-handle"
                data-resizing={activeDirection === 'width' ? 'true' : undefined}
                onMouseDown={(event) => startResize(event, 'width')}
                onDoubleClick={(event) => handleResetSize('width', event)}
                style={{
                    position: 'absolute',
                    top: 0,
                    right: 0,
                    bottom: '12px',
                    width: '10px',
                    cursor: 'ew-resize',
                    zIndex: 4,
                    touchAction: 'none',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'flex-end',
                    paddingRight: '2px',
                }}
                title={`Resize ${panelLabel} width`}
            >
                <span className="sp-resize-pill" aria-hidden="true" style={{ width: '2px', height: '24px', borderRadius: '999px', background: handleColor, opacity: 0.45, pointerEvents: 'none' }} />
            </div>
            <div
                className="sp-resize-handle"
                data-resizing={activeDirection === 'height' ? 'true' : undefined}
                onMouseDown={(event) => startResize(event, 'height')}
                onDoubleClick={(event) => handleResetSize('height', event)}
                style={{
                    position: 'absolute',
                    left: 0,
                    right: '12px',
                    bottom: 0,
                    height: '10px',
                    cursor: 'ns-resize',
                    zIndex: 4,
                    touchAction: 'none',
                    display: 'flex',
                    alignItems: 'flex-end',
                    justifyContent: 'center',
                    paddingBottom: '2px',
                }}
                title={`Resize ${panelLabel} height`}
            >
                <span className="sp-resize-pill" aria-hidden="true" style={{ width: '28px', height: '2px', borderRadius: '999px', background: handleColor, opacity: 0.45, pointerEvents: 'none' }} />
            </div>
            <div
                className="sp-resize-handle"
                data-resizing={activeDirection === 'both' ? 'true' : undefined}
                onMouseDown={(event) => startResize(event, 'both')}
                onDoubleClick={(event) => handleResetSize('both', event)}
                style={{
                    position: 'absolute',
                    right: 0,
                    bottom: 0,
                    width: '14px',
                    height: '14px',
                    cursor: 'nwse-resize',
                    zIndex: 5,
                    touchAction: 'none',
                }}
                title={`Resize ${panelLabel}`}
            >
                <div
                    className="sp-resize-pill"
                    aria-hidden="true"
                    style={{
                        position: 'absolute',
                        right: '4px',
                        bottom: '4px',
                        width: '3px',
                        height: '3px',
                        borderRadius: '999px',
                        background: handleColor,
                        opacity: 0.55,
                        boxShadow: `-4px 0 0 ${handleColor}, 0 -4px 0 ${handleColor}, -4px -4px 0 ${handleColor}`,
                        pointerEvents: 'none',
                    }}
                />
            </div>
        </div>
    );
}

export default ResizableFieldPanel;
