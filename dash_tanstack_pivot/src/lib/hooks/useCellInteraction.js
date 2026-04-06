import { useState } from 'react';

/**
 * useCellInteraction — extracts cell selection, drag, fill, and undo state
 * from DashTanstackPivot.
 *
 * Covers: context menu, cell selection, drag-select, fill-down, row-select,
 * and undo/redo history.
 */
export function useCellInteraction() {
    const [contextMenu, setContextMenu] = useState(null);
    const [selectedCells, setSelectedCells] = useState({});
    const [lastSelected, setLastSelected] = useState(null);
    const [isDragging, setIsDragging] = useState(false);
    const [dragStart, setDragStart] = useState(null);
    const [isFilling, setIsFilling] = useState(false);
    const [fillRange, setFillRange] = useState(null);
    const [history, setHistory] = useState([]);
    const [future, setFuture] = useState([]);
    const [isRowSelecting, setIsRowSelecting] = useState(false);
    const [rowDragStart, setRowDragStart] = useState(null);

    return {
        contextMenu, setContextMenu,
        selectedCells, setSelectedCells,
        lastSelected, setLastSelected,
        isDragging, setIsDragging,
        dragStart, setDragStart,
        isFilling, setIsFilling,
        fillRange, setFillRange,
        history, setHistory,
        future, setFuture,
        isRowSelecting, setIsRowSelecting,
        rowDragStart, setRowDragStart,
    };
}
