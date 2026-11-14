import { useCallback, useRef, useState } from 'react';

/**
 * Owns the interaction lifecycle shared by sidebar field zones and table
 * headers: drag payloads, drop indicators, semantic field moves, rendered
 * column reordering, and column pinning. The composition root supplies its
 * persisted layout writers and table metadata explicitly through `configure`.
  @returns {any} */
export function usePivotFieldInteractions() {
    /** @type {any} */
    const contextRef = useRef({});
    /** @type {any} */
    const keyboardOrderRef = useRef(null);
    /** @type {any} */
    const dragItemRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [dragItem, setDragItem] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [dropLine, setDropLine] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [dragOverHeaderId, setDragOverHeaderId] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [dragOverSide, setDragOverSide] = useState(null);

    /** @type {(...args: any[]) => any} */
    const configure = useCallback((context) => {
        contextRef.current = context && typeof context === 'object' ? context : {};
    }, []);

    /** @type {(...args: any[]) => any} */
    const matchesValueConfigId = useCallback((columnId, valueConfig) => {
        if (!columnId || !valueConfig || !valueConfig.field) return false;
        if (valueConfig.agg === 'formula') {
            return columnId === valueConfig.field || columnId.endsWith(`_${valueConfig.field}`);
        }
        /** @type {any} */
        const flatMeasureId = valueConfig.alias || valueConfig.id || `${valueConfig.field}_${valueConfig.agg}`;
        return columnId === flatMeasureId
            || columnId.endsWith(`_${flatMeasureId}`)
            || columnId.endsWith(`_${valueConfig.field}_${valueConfig.agg}`);
    }, []);

    /** @type {(...args: any[]) => any} */
    const getFieldZone = useCallback((id) => {
        const { rowFields, colFields, valConfigs } = contextRef.current;
        if (id === 'hierarchy') return 'rows';
        if (Array.isArray(rowFields) && rowFields.includes(id)) return 'rows';
        if (Array.isArray(colFields) && colFields.includes(id)) return 'cols';
        if (Array.isArray(valConfigs) && valConfigs.some((config) => matchesValueConfigId(id, config))) return 'vals';
        return null;
    }, [matchesValueConfigId]);

    /** @type {(...args: any[]) => any} */
    const getFieldIndex = useCallback((id, zone) => {
        const { rowFields, colFields, valConfigs } = contextRef.current;
        if (zone === 'rows') return Array.isArray(rowFields) ? rowFields.indexOf(id) : -1;
        if (zone === 'cols') return Array.isArray(colFields) ? colFields.indexOf(id) : -1;
        if (zone === 'vals') {
            return Array.isArray(valConfigs)
                ? valConfigs.findIndex((config) => matchesValueConfigId(id, config))
                : -1;
        }
        return -1;
    }, [matchesValueConfigId]);

    /** @type {(...args: any[]) => any} */
    const clearDragState = useCallback(() => {
        dragItemRef.current = null;
        setDragItem(null);
        setDropLine(null);
        setDragOverHeaderId(null);
        setDragOverSide(null);
    }, []);

    /** @type {(...args: any[]) => any} */
    const handlePinColumn = useCallback((columnId, side) => {
        const { tableRef, getAllLeafIdsFromColumn, setColumnPinningWithHistory } = contextRef.current;
        /** @type {any} */
        const table = tableRef && tableRef.current;
        if (!table || !columnId || typeof setColumnPinningWithHistory !== 'function') return false;

        /** @type {any} */
        const column = table.getColumn(columnId);
        if (!column) return false;

        /** @type {any} */
        const idsToUpdate = new Set();
        /** @type {any} */
        const isGroup = column.columns && column.columns.length > 0;
        /** @type {any} */
        const leafIds = isGroup && typeof getAllLeafIdsFromColumn === 'function'
            ? getAllLeafIdsFromColumn(column)
            : [columnId];
        leafIds.forEach(/** @param {any} [id] @returns {any} */ (id) => idsToUpdate.add(id));

        if (String(columnId).startsWith('group_')) {
            /** @type {any} */
            const rawPathKey = String(columnId).replace('group_', '');
            idsToUpdate.add(`${rawPathKey}_collapsed`);
        }

        /** @type {any} */
        const idsArray = Array.from(idsToUpdate);
        setColumnPinningWithHistory(/** @param {any} [previousPinning] @returns {any} */ (previousPinning) => {
            /** @type {Record<string, any>} */
            const nextPinning = {
                left: [...((previousPinning && previousPinning.left) || [])],
                right: [...((previousPinning && previousPinning.right) || [])],
            };
            nextPinning.left = nextPinning.left.filter(/** @param {any} [id] @returns {any} */ (id) => !idsToUpdate.has(id));
            nextPinning.right = nextPinning.right.filter(/** @param {any} [id] @returns {any} */ (id) => !idsToUpdate.has(id));
            if (side === 'left') nextPinning.left.push(...idsArray);
            if (side === 'right') nextPinning.right.push(...idsArray);
            return nextPinning;
        }, 'layout:column-pinning');
        return true;
    }, []);

    /** @type {(...args: any[]) => any} */
    const toggleAllColumnsPinned = useCallback((pinState) => {
        const { table, layoutMode, setColumnPinningWithHistory } = contextRef.current;
        if (!table || typeof table.getAllLeafColumns !== 'function' || typeof setColumnPinningWithHistory !== 'function') {
            return false;
        }
        /** @type {any} */
        const leafColumns = table.getAllLeafColumns();
        /** @type {Record<string, any>} */
        const nextPinning = { left: [], right: [] };
        if (pinState === 'left') {
            nextPinning.left = leafColumns.map(/** @param {any} [column] @returns {any} */ (column) => column.id).filter(/** @param {any} [id] @returns {any} */ (id) => id !== 'no_data');
        } else if (pinState === 'right') {
            nextPinning.right = leafColumns.map(/** @param {any} [column] @returns {any} */ (column) => column.id).filter(/** @param {any} [id] @returns {any} */ (id) => id !== 'no_data');
        } else if (layoutMode === 'hierarchy') {
            nextPinning.left = ['hierarchy'];
        }
        setColumnPinningWithHistory(nextPinning, 'layout:toggle-all-columns-pinned');
        return true;
    }, []);

    /** @type {(...args: any[]) => any} */
    const applyFieldZoneMove = useCallback(({
        field,
        srcZone,
        srcIdx = -1,
        targetZone,
        targetIdx = 0,
        source = 'layout:zone-drop',
    } = {}) => {
        const context = contextRef.current;
        const {
            rowFields,
            colFields,
            valConfigs,
            filters,
            measureAxis,
            sparklineFields,
            isCustomCategoryField,
            normalizeMeasureAxisValue,
            requestLayoutHistoryCapture,
            setMeasureAxis,
            setRowFieldsWithHistory,
            setColFieldsWithHistory,
            setValConfigsWithHistory,
            setFilters,
            showNotification,
        } = context;
        /** @type {any} */
        const draggedValueConfig = typeof field === 'object' && field ? field : null;
        /** @type {any} */
        const isFormulaValue = Boolean(draggedValueConfig && draggedValueConfig.agg === 'formula');
        /** @type {any} */
        const fieldName = typeof field === 'string' ? field : field && field.field;
        if (!fieldName || typeof fieldName !== 'string') return false;
        if (!['rows', 'cols', 'vals', 'filter'].includes(targetZone)) return false;
        if (targetZone === 'vals' && typeof isCustomCategoryField === 'function' && isCustomCategoryField(fieldName)) {
            if (typeof showNotification === 'function') {
                showNotification('Custom categories are dimensions. Use them in Rows, Columns, or Filters.', 'warning');
            }
            return false;
        }

        /** @type {any} */
        const numericTargetIdx = Number(targetIdx);
        /** @type {any} */
        const safeTargetIdx = Number.isFinite(numericTargetIdx) ? Math.max(0, Math.floor(numericTargetIdx)) : 0;
        /** @param {any} [list] @param {any} [index] @param {any} [item] @returns {any} */
        const insertItem = (list, index, item) => {
            /** @type {any[]} */
            const next = [...(Array.isArray(list) ? list : [])];
            next.splice(Math.min(Math.max(index, 0), next.length), 0, item);
            return next;
        };
        /** @param {any} [list] @param {any} [index] @param {any} [target] @param {any} [setList] @returns {any} */
        const moveItem = (list, index, target, setList) => {
            if (!Array.isArray(list) || !Number.isInteger(index) || index < 0 || index >= list.length) return false;
            /** @type {any[]} */
            const next = [...list];
            /** @type {any} */
            const [moved] = next.splice(index, 1);
            /** @type {any} */
            let insertAt = Math.min(Math.max(target, 0), next.length);
            if (index < target) insertAt = Math.max(0, insertAt - 1);
            next.splice(insertAt, 0, moved);
            setList(next);
            return true;
        };
        /** @returns {any} */
        const defaultValueConfig = () => {
            /** @type {any} */
            const autoSparklineConfig = sparklineFields && sparklineFields[fieldName];
            return autoSparklineConfig
                ? { field: fieldName, agg: 'array_agg', sparkline: { ...autoSparklineConfig } }
                : { field: fieldName, agg: 'sum' };
        };
        /** @type {any} */
        const normalizedAxis = typeof normalizeMeasureAxisValue === 'function'
            ? normalizeMeasureAxisValue(measureAxis)
            : {};
        /** @type {any} */
        const measureAxisLabelField = normalizedAxis.labelField || 'Measure Name';
        /** @type {any} */
        const draggedValueIndex = Number.isInteger(srcIdx) && srcIdx >= 0
            ? srcIdx
            : (Array.isArray(valConfigs) ? valConfigs.findIndex((config) => matchesValueConfigId(fieldName, config)) : -1);
        /** @type {any} */
        const sourceValueConfig = draggedValueConfig || (draggedValueIndex >= 0 ? valConfigs[draggedValueIndex] : null);
        /** @type {any} */
        const isMeasureAxisLabelField = fieldName === measureAxisLabelField
            && (srcZone === 'measureAxis' || normalizedAxis.placement !== 'none');
        /** @type {any} */
        const isValueMeasureDragToAxis = Boolean(
            sourceValueConfig
            && (srcZone === 'vals' || draggedValueConfig)
            && (targetZone === 'rows' || targetZone === 'cols')
        );
        /** @param {any} [placement] @returns {any} */
        const placeMeasureAxisLabel = (placement) => {
            /** @param {any} [list] @param {any} [index] @returns {any} */
            const insertLabel = (list, index) => insertItem(
                (list || []).filter(/** @param {any} [item] @returns {any} */ (item) => item !== measureAxisLabelField),
                index,
                measureAxisLabelField
            );
            if (typeof requestLayoutHistoryCapture === 'function') requestLayoutHistoryCapture(source);
            if (typeof setMeasureAxis === 'function') {
                setMeasureAxis(/** @param {any} [previous] @returns {any} */ (previous) => ({
                    ...(typeof normalizeMeasureAxisValue === 'function' ? normalizeMeasureAxisValue(previous) : previous),
                    placement,
                    labelField: measureAxisLabelField,
                }));
            }
            if (placement === 'rows') {
                if (typeof setRowFieldsWithHistory === 'function') setRowFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => insertLabel(previous, safeTargetIdx), source);
                if (typeof setColFieldsWithHistory === 'function') setColFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => (previous || []).filter(/** @param {any} [item] @returns {any} */ (item) => item !== measureAxisLabelField), source);
            } else if (placement === 'columns') {
                if (typeof setColFieldsWithHistory === 'function') setColFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => insertLabel(previous, safeTargetIdx), source);
                if (typeof setRowFieldsWithHistory === 'function') setRowFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => (previous || []).filter(/** @param {any} [item] @returns {any} */ (item) => item !== measureAxisLabelField), source);
            }
            if (typeof showNotification === 'function') {
                showNotification(`Showing measure names in ${placement === 'rows' ? 'Rows' : 'Columns'}.`, 'success');
            }
        };

        if (isMeasureAxisLabelField) {
            if (targetZone === 'rows' || targetZone === 'cols') {
                placeMeasureAxisLabel(targetZone === 'rows' ? 'rows' : 'columns');
                return true;
            }
            if (typeof showNotification === 'function') showNotification('Measure names can be placed in Rows or Columns.', 'warning');
            return false;
        }
        if (isValueMeasureDragToAxis) {
            placeMeasureAxisLabel(targetZone === 'rows' ? 'rows' : 'columns');
            return true;
        }
        if (isFormulaValue && targetZone !== 'vals') {
            if (typeof showNotification === 'function') showNotification('Formula columns can only stay in Values.', 'warning');
            return false;
        }

        if (srcZone !== targetZone) {
            if (srcZone === 'rows' && typeof setRowFieldsWithHistory === 'function') setRowFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => previous.filter(/** @param {any} [item] @returns {any} */ (item) => item !== fieldName), source);
            if (srcZone === 'cols' && typeof setColFieldsWithHistory === 'function') setColFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => previous.filter(/** @param {any} [item] @returns {any} */ (item) => item !== fieldName), source);
            if (srcZone === 'vals' && typeof setValConfigsWithHistory === 'function') {
                setValConfigsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => (
                    Number.isInteger(srcIdx) && srcIdx >= 0
                        ? previous.filter(/** @param {any} [_] @param {any} [index] @returns {any} */ (_, index) => index !== srcIdx)
                        : previous.filter(/** @param {any} [config] @returns {any} */ (config) => config.field !== fieldName)
                ), source);
            }
            if (targetZone === 'rows' && typeof setRowFieldsWithHistory === 'function') setRowFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => (previous.includes(fieldName) ? previous : insertItem(previous, safeTargetIdx, fieldName)), source);
            if (targetZone === 'cols' && typeof setColFieldsWithHistory === 'function') setColFieldsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => (previous.includes(fieldName) ? previous : insertItem(previous, safeTargetIdx, fieldName)), source);
            if (targetZone === 'vals' && typeof setValConfigsWithHistory === 'function') {
                setValConfigsWithHistory(/** @param {any} [previous] @returns {any} */ (previous) => insertItem(previous, safeTargetIdx, draggedValueConfig ? { ...draggedValueConfig } : defaultValueConfig()), source);
            }
            if (targetZone === 'filter' && !Object.prototype.hasOwnProperty.call(filters || {}, fieldName)) {
                if (typeof requestLayoutHistoryCapture === 'function') requestLayoutHistoryCapture(source);
                if (typeof setFilters === 'function') setFilters(/** @param {any} [previous] @returns {any} */ (previous) => ({ ...(previous || {}), [fieldName]: '' }));
            }
            return true;
        }
        if (targetZone === 'rows' && typeof setRowFieldsWithHistory === 'function') return moveItem(rowFields, srcIdx, safeTargetIdx, /** @param {any} [next] @returns {any} */ (next) => setRowFieldsWithHistory(next, source));
        if (targetZone === 'cols' && typeof setColFieldsWithHistory === 'function') return moveItem(colFields, srcIdx, safeTargetIdx, /** @param {any} [next] @returns {any} */ (next) => setColFieldsWithHistory(next, source));
        if (targetZone === 'vals' && typeof setValConfigsWithHistory === 'function') return moveItem(valConfigs, srcIdx, safeTargetIdx, /** @param {any} [next] @returns {any} */ (next) => setValConfigsWithHistory(next, source));
        return false;
    }, [matchesValueConfigId]);

    /** @type {(...args: any[]) => any} */
    const resolveDraggedHeaderItem = useCallback((event) => {
        if (dragItem) return dragItem;
        if (!event || !event.dataTransfer) return null;
        try {
            /** @type {any} */
            const rawPayload = event.dataTransfer.getData('application/x-pivot-column');
            if (!rawPayload) return null;
            /** @type {any} */
            const parsed = JSON.parse(rawPayload);
            return parsed && typeof parsed === 'object' && parsed.field ? parsed : null;
        } catch (error) {
            return null;
        }
    }, [dragItem]);

    /** @type {(...args: any[]) => any} */
    const resolveHeaderDropPinSide = useCallback((event) => {
        const { parentRef } = contextRef.current;
        /** @type {any} */
        const container = parentRef && parentRef.current;
        if (!container || !event || !Number.isFinite(Number(event.clientX))) return null;
        /** @type {any} */
        const rect = container.getBoundingClientRect();
        if (!rect || !Number.isFinite(Number(rect.left)) || !Number.isFinite(Number(rect.right))) return null;
        /** @type {any} */
        const edgeWidth = Math.min(112, Math.max(44, rect.width * 0.1));
        if (event.clientX <= rect.left + edgeWidth) return 'left';
        if (event.clientX >= rect.right - edgeWidth) return 'right';
        return null;
    }, []);

    /** @type {(...args: any[]) => any} */
    const resolveHeaderDropPosition = useCallback((event) => {
        /** @type {any} */
        const target = event && event.currentTarget;
        /** @type {any} */
        const rect = target && typeof target.getBoundingClientRect === 'function' ? target.getBoundingClientRect() : null;
        if (!rect || !Number.isFinite(Number(event.clientX))) return 'before';
        return event.clientX > rect.left + (rect.width / 2) ? 'after' : 'before';
    }, []);

    /** @type {(...args: any[]) => any} */
    const onHeaderDrop = useCallback((event, targetColumnId) => {
        event.preventDefault();
        event.stopPropagation();
        const context = contextRef.current;
        /** @type {any} */
        const draggedItem = resolveDraggedHeaderItem(event);
        if (!draggedItem) return;
        const { field } = draggedItem;
        /** @type {any} */
        const draggedValueConfig = typeof field === 'object' && field ? field : null;
        /** @type {any} */
        const isFormulaValue = Boolean(draggedValueConfig && draggedValueConfig.agg === 'formula');
        /** @type {any} */
        const fieldName = typeof field === 'string' ? field : field && field.field;
        /** @type {any} */
        const sourceColumn = context.tableRef && context.tableRef.current && fieldName
            ? context.tableRef.current.getColumn(fieldName)
            : null;
        /** @type {any} */
        const dropPinSide = resolveHeaderDropPinSide(event);
        /** @type {any} */
        const dropPosition = resolveHeaderDropPosition(event);
        /** @type {any} */
        const targetZone = getFieldZone(targetColumnId);

        if (!fieldName || !targetColumnId) {
            clearDragState();
            return;
        }
        if (isFormulaValue) {
            if (typeof context.showNotification === 'function') context.showNotification('Formula columns can be reordered inside Values only.', 'warning');
            clearDragState();
            return;
        }
        if (targetZone === 'vals' && typeof context.isCustomCategoryField === 'function' && context.isCustomCategoryField(fieldName)) {
            if (typeof context.showNotification === 'function') context.showNotification('Custom categories are dimensions. Use them in Rows, Columns, or Filters.', 'warning');
            clearDragState();
            return;
        }

        /** @type {any} */
        const canPinDraggedColumn = sourceColumn && fieldName !== '__row_number__' && fieldName !== '__report_config__';
        if (dropPinSide && canPinDraggedColumn) {
            handlePinColumn(fieldName, dropPinSide);
            if (typeof context.setAnnouncement === 'function' && typeof context.getLocaleText === 'function') {
                context.setAnnouncement(context.getLocaleText('columnPinnedAnnouncement', '{label} pinned {side}.', {
                    label: context.formatDisplayLabel(fieldName), side: dropPinSide,
                }));
            }
        } else if (canPinDraggedColumn && typeof context.getPinningState === 'function') {
            /** @type {any} */
            const targetIsPinned = context.getPinningState(targetColumnId);
            /** @type {any} */
            const sourceIsPinned = context.getPinningState(fieldName);
            if (targetIsPinned && targetIsPinned !== sourceIsPinned) handlePinColumn(fieldName, targetIsPinned);
            else if (!targetIsPinned && sourceIsPinned) handlePinColumn(fieldName, false);
        }

        /** @type {any} */
        const sourceZone = draggedItem.zone || getFieldZone(fieldName);
        /** @type {any} */
        const sourceIndex = sourceZone ? getFieldIndex(fieldName, sourceZone) : -1;
        /** @type {any} */
        const targetIndex = targetZone ? getFieldIndex(targetColumnId, targetZone) : -1;
        /** @type {any} */
        const canApplySemanticMove = Boolean(sourceZone && targetZone && sourceIndex >= 0 && targetIndex >= 0);
        /** @type {any} */
        let targetInsertIndex = dropPosition === 'after' ? targetIndex + 1 : targetIndex;
        if (sourceZone === targetZone && sourceIndex >= 0 && sourceIndex < targetInsertIndex) targetInsertIndex -= 1;
        /** @type {any} */
        const didApplySemanticMove = canApplySemanticMove
            ? applyFieldZoneMove({
                field,
                srcZone: sourceZone,
                srcIdx: sourceIndex,
                targetZone,
                targetIdx: targetInsertIndex,
                source: 'layout:header-drop',
            })
            : false;

        if (!didApplySemanticMove && fieldName !== targetColumnId
            && typeof context.setColumnOrderWithHistory === 'function'
            && typeof context.mergeColumnOrderWithIds === 'function'
            && typeof context.moveColumnIdInOrder === 'function') {
            context.setColumnOrderWithHistory(/** @param {any} [previousOrder] @returns {any} */ (previousOrder) => {
                /** @type {any} */
                const baseOrder = context.mergeColumnOrderWithIds(previousOrder, context.availableColumnOrderIds || []);
                return context.moveColumnIdInOrder(baseOrder, fieldName, targetColumnId, dropPosition);
            }, 'layout:header-reorder');
            if (typeof context.setAnnouncement === 'function' && typeof context.getLocaleText === 'function') {
                context.setAnnouncement(context.getLocaleText('columnMovedAnnouncement', '{label} moved {position} {target}.', {
                    label: context.formatDisplayLabel(fieldName),
                    position: dropPosition,
                    target: context.formatDisplayLabel(targetColumnId),
                }));
            }
        }
        clearDragState();
    }, [applyFieldZoneMove, clearDragState, getFieldIndex, getFieldZone, handlePinColumn, resolveDraggedHeaderItem, resolveHeaderDropPinSide, resolveHeaderDropPosition]);

    /** @type {(...args: any[]) => any} */
    const onDragStart = useCallback((event, field, zone, index) => {
        /** @type {any} */
        const fieldName = typeof field === 'string' ? field : field && field.field;
        /** @type {any} */
        const resolvedZone = getFieldZone(fieldName) || zone;
        /** @type {any} */
        const resolvedIndex = Number.isInteger(index) && index >= 0 ? index : getFieldIndex(fieldName, resolvedZone);
        /** @type {Record<string, any>} */
        const nextDragItem = { field, zone: resolvedZone, idx: resolvedIndex };
        dragItemRef.current = nextDragItem;
        setDragItem(nextDragItem);
        if (event && event.dataTransfer) {
            event.dataTransfer.effectAllowed = 'move';
            try {
                event.dataTransfer.setData('application/x-pivot-column', JSON.stringify({
                    field: fieldName, zone: resolvedZone, idx: resolvedIndex,
                }));
                event.dataTransfer.setData('text/plain', fieldName || '');
            } catch (error) {
                // Some browsers restrict custom drag payloads; local React state still carries the drag.
            }
        }
    }, [getFieldIndex, getFieldZone]);

    /** @type {(...args: any[]) => any} */
    const onDragOver = useCallback((event, zone, index) => {
        event.preventDefault();
        if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
        if (['rows', 'cols', 'vals', 'filter'].includes(zone) && event.currentTarget) {
            /** @type {any} */
            const rect = event.currentTarget.getBoundingClientRect();
            /** @type {any} */
            const midpoint = rect.top + rect.height / 2;
            setDropLine({ zone, idx: event.clientY > midpoint ? index + 1 : index });
        }
    }, []);

    /** @type {(...args: any[]) => any} */
    const onHeaderDragOver = useCallback((event, columnId) => {
        /** @type {any} */
        const hasHeaderDragPayload = Boolean(
            dragItem
            || (
                event.dataTransfer
                && event.dataTransfer.types
                && Array.from(event.dataTransfer.types).includes('application/x-pivot-column')
            )
        );
        if (!hasHeaderDragPayload) return;
        event.preventDefault();
        event.stopPropagation();
        if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
        if (columnId && event.currentTarget) {
            /** @type {any} */
            const rect = event.currentTarget.getBoundingClientRect();
            setDragOverHeaderId(columnId);
            setDragOverSide(event.clientX < rect.left + rect.width / 2 ? 'before' : 'after');
        }
    }, [dragItem]);

    /** @type {(...args: any[]) => any} */
    const onHeaderDragEnd = useCallback(() => {
        clearDragState();
    }, [clearDragState]);

    /** @type {(...args: any[]) => any} */
    const resolveSidebarDropItem = useCallback((event) => {
        if (dragItemRef.current) return dragItemRef.current;
        if (dragItem) return dragItem;
        if (!event || !event.dataTransfer) return null;
        try {
            /** @type {any} */
            const rawPayload = event.dataTransfer.getData('application/x-pivot-column');
            if (rawPayload) {
                /** @type {any} */
                const parsed = JSON.parse(rawPayload);
                if (parsed && typeof parsed.field === 'string' && parsed.field) {
                    /** @type {any} */
                    const zone = parsed.zone || getFieldZone(parsed.field) || 'pool';
                    return {
                        field: parsed.field,
                        zone,
                        idx: Number.isInteger(parsed.idx) ? parsed.idx : getFieldIndex(parsed.field, zone),
                    };
                }
            }
        } catch (error) {
            // Fall back to text/plain below.
        }
        try {
            /** @type {any} */
            const fieldName = event.dataTransfer.getData('text/plain');
            if (fieldName) {
                /** @type {any} */
                const zone = getFieldZone(fieldName) || 'pool';
                return { field: fieldName, zone, idx: getFieldIndex(fieldName, zone) };
            }
        } catch (error) {
            return null;
        }
        return null;
    }, [dragItem, getFieldIndex, getFieldZone]);

    /** @type {(...args: any[]) => any} */
    const onDrop = useCallback((event, targetZone) => {
        event.preventDefault();
        event.stopPropagation();
        /** @type {any} */
        const resolvedDragItem = resolveSidebarDropItem(event);
        if (!resolvedDragItem) {
            clearDragState();
            return;
        }
        const { field, zone: srcZone, idx: srcIdx } = resolvedDragItem;
        /** @type {any} */
        const targetIndex = dropLine && dropLine.zone === targetZone ? dropLine.idx : 0;
        applyFieldZoneMove({ field, srcZone, srcIdx, targetZone, targetIdx: targetIndex, source: 'layout:zone-drop' });
        clearDragState();
    }, [applyFieldZoneMove, clearDragState, dropLine, resolveSidebarDropItem]);

    /** @type {(...args: any[]) => any} */
    const onHeaderKeyboardMove = useCallback((columnId, delta) => {
        const context = contextRef.current;
        if (!columnId || !(context.availableColumnOrderIds || []).includes(columnId)) return false;
        if (typeof context.mergeColumnOrderWithIds !== 'function'
            || typeof context.moveColumnIdByDelta !== 'function'
            || typeof context.areStringArraysEqual !== 'function'
            || typeof context.setColumnOrderWithHistory !== 'function') return false;
        /** @type {any} */
        const direction = delta < 0 ? -1 : 1;
        /** @type {any} */
        const renderOrder = context.mergeColumnOrderWithIds(context.columnOrder, context.availableColumnOrderIds);
        // Server-side schema order is the order the user can see. The saved
        // cell order can temporarily lag it while a layout response settles.
        /** @type {any} */
        const displayedOrder = context.mergeColumnOrderWithIds(
            context.effectiveColumnOrder || context.columnOrder, context.availableColumnOrderIds);
        const pending = keyboardOrderRef.current;
        if (pending && (context.areStringArraysEqual(displayedOrder, pending.next)
            || (!context.areStringArraysEqual(renderOrder, pending.base)
                && !context.areStringArraysEqual(renderOrder, pending.next)))) {
            keyboardOrderRef.current = null;
        }
        // The server may show a newer column order before React's saved-order
        // state catches up. Consecutive keys before that render use our last
        // issued order; every other move starts from what is displayed.
        /** @type {any} */
        const baseOrder = keyboardOrderRef.current?.next || displayedOrder;
        /** @type {any} */
        const nextOrder = context.moveColumnIdByDelta(baseOrder, columnId, direction);
        const moved = !context.areStringArraysEqual(baseOrder, nextOrder);
        if (typeof window !== 'undefined' && window.__PIVOT_QUALIFY_KEYBOARD_MOVES__ === true) {
            window.__pivotKeyboardMoveTrace = window.__pivotKeyboardMoveTrace || [];
            window.__pivotKeyboardMoveTrace.push({
                at: performance.now(), columnId, direction, renderOrder, displayedOrder,
                pendingOrder: pending?.next || null, baseOrder, nextOrder, moved,
                availableColumnOrderIds: context.availableColumnOrderIds,
            });
        }
        if (moved) {
            keyboardOrderRef.current = { base: baseOrder, next: nextOrder };
            context.setColumnOrderWithHistory(nextOrder, 'layout:header-keyboard-reorder');
        }
        if (moved && typeof context.setAnnouncement === 'function' && typeof context.getLocaleText === 'function') {
            context.setAnnouncement(context.getLocaleText('columnMovedByKeyboardAnnouncement', '{label} moved {direction}.', {
                label: context.formatDisplayLabel(columnId),
                direction: direction < 0 ? 'left' : 'right',
            }));
        }
        return moved;
    }, []);

    /** @type {(...args: any[]) => any} */
    const onHeaderKeyboardPin = useCallback((columnId, side) => {
        const context = contextRef.current;
        if (!columnId || columnId === '__row_number__' || columnId === '__report_config__') return false;
        if (!handlePinColumn(columnId, side)) return false;
        if (typeof context.setAnnouncement === 'function' && typeof context.getLocaleText === 'function') {
            context.setAnnouncement(context.getLocaleText('columnPinnedAnnouncement', '{label} pinned {side}.', {
                label: context.formatDisplayLabel(columnId),
                side: side || 'center',
            }));
        }
        return true;
    }, [handlePinColumn]);

    return {
        configure,
        dragItem,
        dropLine,
        dragOverHeaderId,
        dragOverSide,
        handlePinColumn,
        toggleAllColumnsPinned,
        applyFieldZoneMove,
        onDragStart,
        onDragOver,
        onDrop,
        onHeaderDrop,
        onHeaderDragOver,
        onHeaderDragEnd,
        onHeaderKeyboardMove,
        onHeaderKeyboardPin,
    };
}
