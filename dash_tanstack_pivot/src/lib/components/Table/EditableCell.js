import React, { useState, useEffect, useRef } from 'react';
import { formatValue } from '../../utils/helpers';
import { CELL_CONTENT_RESET_STYLE } from '../../utils/styles';
import { useOptionalPivotValueDisplay } from '../../contexts/PivotValueDisplayContext';
import {
    coerceEditorValue,
    formatEditorDisplayValue,
    normalizeEditorOptions,
    normalizeEditorType,
    toEditorInputValue,
    validateEditorValue,
} from '../../utils/editing';

const EditableCell = ({
    getValue,
    row,
    column,
    columnConfig,
    displayValue,
    format,
    numberGroupSeparator,
    validationRules,
    onCellEdit,
    onEditBlocked,
    setProps,
    handleContextMenu,
    editingDisabled = false,
    editingDisabledReason = null,
    rowEditMode = 'cell',
    editorConfig = null,
    rowEditSession = null,
    onRequestRowStart,
    onRowDraftChange,
    onRequestRowSave,
    onRequestRowCancel,
    requestEditorOptions,
    editorOptions = [],
    editorOptionsLoading = false,
    editorOptionsError = null,
}) => {
    const rawValue = getValue();
    const rowPath = (row && row.original && row.original._path) || row.id;
    const valueDisplayContext = useOptionalPivotValueDisplay();
    const currentCellValue = valueDisplayContext && typeof valueDisplayContext.resolveCurrentCellValue === 'function'
        ? valueDisplayContext.resolveCurrentCellValue(rowPath, column.id, rawValue)
        : rawValue;
    const contextDisplayValue = valueDisplayContext && typeof valueDisplayContext.resolveCellDisplayValue === 'function'
        ? valueDisplayContext.resolveCellDisplayValue(rowPath, column.id, displayValue !== undefined ? displayValue : rawValue)
        : undefined;
    const resolvedDisplayValue = contextDisplayValue !== undefined
        ? contextDisplayValue
        : (displayValue !== undefined ? displayValue : rawValue);
    const isRowEditing = Boolean(rowEditSession && rowEditSession.active);
    const isOriginalMode = Boolean(
        valueDisplayContext
        && valueDisplayContext.editValueDisplayMode === 'original'
        && !isRowEditing
    );
    const normalizedOptions = normalizeEditorOptions(
        Array.isArray(editorOptions) && editorOptions.length > 0
            ? editorOptions
            : (editorConfig && Array.isArray(editorConfig.options) ? editorConfig.options : []),
    );
    const supportsRowEditSession = rowEditMode === 'row' || rowEditMode === 'hybrid';
    const resolvedEditorConfig = {
        ...(editorConfig && typeof editorConfig === 'object' ? editorConfig : {}),
        options: normalizedOptions,
    };
    const editorType = normalizeEditorType(resolvedEditorConfig.editor, currentCellValue);
    const effectiveEditingDisabled = editingDisabled || !resolvedEditorConfig || resolvedEditorConfig.editable === false;
    const rowSessionDraft = rowEditSession && rowEditSession.drafts && Object.prototype.hasOwnProperty.call(rowEditSession.drafts, column.id)
        ? rowEditSession.drafts[column.id]
        : undefined;
    const rowSessionError = rowEditSession && rowEditSession.errors ? rowEditSession.errors[column.id] : null;
    const rowSessionFocusColumnId = rowEditSession && rowEditSession.focusColumnId
        ? String(rowEditSession.focusColumnId)
        : null;
    const shouldRenderRowEditorInThisCell = Boolean(
        isRowEditing
        && rowEditSession
        && rowEditSession.status !== 'saving'
        && (
            !rowEditSession.autoSave
            || !rowSessionFocusColumnId
            || rowSessionFocusColumnId === column.id
            || rowSessionDraft !== undefined
            || Boolean(rowSessionError)
        )
    );
    const [submittedDisplayValue, setSubmittedDisplayValue] = useState(null);
    const pendingDisplayResetRef = useRef(null);
    const datalistIdRef = useRef(`pivot-editor-${Math.random().toString(36).slice(2, 10)}`);
    const effectiveDisplayValue = (
        !isRowEditing
        && submittedDisplayValue !== null
        && !isOriginalMode
    )
        ? submittedDisplayValue
        : (
            rowSessionDraft !== undefined
                ? rowSessionDraft
                : resolvedDisplayValue
        );
    const [value, setValue] = useState(toEditorInputValue(effectiveDisplayValue, resolvedEditorConfig));
    const [isEditing, setIsEditing] = useState(Boolean(shouldRenderRowEditorInThisCell));
    const [error, setError] = useState(rowSessionError || null);
    const inputRef = useRef(null);
    const lastExternalInputValueRef = useRef(toEditorInputValue(effectiveDisplayValue, resolvedEditorConfig));
    const lastBlockedNoticeRef = useRef(0);

    const mergedValidationRules = Array.isArray(resolvedEditorConfig.validationRules) && resolvedEditorConfig.validationRules.length > 0
        ? resolvedEditorConfig.validationRules
        : (
            validationRules && validationRules[column.id]
                ? validationRules[column.id]
                : []
        );

    useEffect(() => {
        const nextExternalInputValue = toEditorInputValue(effectiveDisplayValue, resolvedEditorConfig);
        const previousExternalInputValue = lastExternalInputValueRef.current;
        const externalChanged = String(nextExternalInputValue) !== String(previousExternalInputValue);
        if (!isEditing || externalChanged) {
            setValue(nextExternalInputValue);
        }
        lastExternalInputValueRef.current = nextExternalInputValue;
    }, [effectiveDisplayValue, isEditing, resolvedEditorConfig]);

    useEffect(() => {
        if (isEditing && inputRef.current) {
            inputRef.current.focus();
            if (typeof inputRef.current.select === 'function' && editorType !== 'checkbox' && editorType !== 'select') {
                inputRef.current.select();
            }
        }
    }, [editorType, isEditing]);

    useEffect(() => {
        if (!effectiveEditingDisabled || !isEditing) return;
        setIsEditing(false);
        setValue(toEditorInputValue(effectiveDisplayValue, resolvedEditorConfig));
    }, [effectiveDisplayValue, effectiveEditingDisabled, isEditing, resolvedEditorConfig]);

    useEffect(() => {
        if (submittedDisplayValue === null) return;
        if (isOriginalMode) {
            setSubmittedDisplayValue(null);
            return;
        }
        if (String(resolvedDisplayValue) === String(submittedDisplayValue)) {
            setSubmittedDisplayValue(null);
        }
    }, [isOriginalMode, resolvedDisplayValue, submittedDisplayValue]);

    useEffect(() => {
        if (!isRowEditing) {
            setIsEditing(false);
            return;
        }
        setIsEditing(shouldRenderRowEditorInThisCell);
    }, [isRowEditing, shouldRenderRowEditorInThisCell]);

    useEffect(() => {
        setError(rowSessionError || null);
    }, [rowSessionError]);

    useEffect(() => {
        if (!isEditing || !requestEditorOptions) return;
        if (!resolvedEditorConfig.optionsSource) return;
        requestEditorOptions(resolvedEditorConfig, column.id, columnConfig || null);
    }, [column.id, columnConfig, isEditing, requestEditorOptions, resolvedEditorConfig]);

    useEffect(() => () => {
        if (pendingDisplayResetRef.current) {
            clearTimeout(pendingDisplayResetRef.current);
        }
    }, []);

    const validate = (nextValue) => validateEditorValue(nextValue, mergedValidationRules, {
        columnId: column.id,
        columnLabel: column.id,
        rowId: rowPath,
        rowValues: isRowEditing
            ? {
                ...((rowEditSession && rowEditSession.originalValues) || {}),
                ...((rowEditSession && rowEditSession.drafts) || {}),
                [column.id]: nextValue,
            }
            : null,
    });

    const getDisplayText = () => formatEditorDisplayValue(
        effectiveDisplayValue,
        resolvedEditorConfig,
        (nextValue) => formatValue(nextValue, format, undefined, numberGroupSeparator),
    );

    const emitInlineUpdate = (nextValue) => {
        const nextUpdate = {
            rowId: rowPath,
            colId: column.id,
            value: nextValue,
            oldValue: currentCellValue,
            rowPath: rowPath || null,
            source: 'inline-edit',
            timestamp: Date.now(),
        };
        if (columnConfig && typeof columnConfig === 'object' && columnConfig.field) {
            nextUpdate.aggregation = {
                field: columnConfig.field,
                agg: columnConfig.agg,
                weightField: columnConfig.weightField || null,
                windowFn: columnConfig.windowFn || null,
            };
        }
        if (typeof onCellEdit === 'function') {
            onCellEdit(nextUpdate);
            return;
        }
        if (setProps) {
            setProps({ cellUpdate: nextUpdate });
        }
    };

    const commitRowDraft = (nextValue) => {
        if (typeof onRowDraftChange !== 'function') return true;
        onRowDraftChange(rowPath, column.id, nextValue, {
            columnConfig: columnConfig || null,
            editorConfig: resolvedEditorConfig,
            currentValue: currentCellValue,
        });
        return true;
    };

    const commitValue = ({ keepEditing = false, nextValue: nextDraftValue = value } = {}) => {
        const nextValue = coerceEditorValue(nextDraftValue, resolvedEditorConfig, currentCellValue);
        const validation = validate(nextValue);
        if (!validation.valid) {
            setError(validation.error || 'Invalid value');
            if (isRowEditing) {
                commitRowDraft(nextValue);
            }
            return false;
        }
        setError(null);

        if (isRowEditing) {
            commitRowDraft(nextValue);
            if (rowEditSession && rowEditSession.autoSave && typeof onRequestRowSave === 'function') {
                setIsEditing(false);
                onRequestRowSave(rowPath);
                return true;
            }
            if (!keepEditing) {
                setIsEditing(false);
            }
            return true;
        }

        if (String(nextValue) !== String(currentCellValue)) {
            setSubmittedDisplayValue(nextValue);
            if (pendingDisplayResetRef.current) {
                clearTimeout(pendingDisplayResetRef.current);
            }
            pendingDisplayResetRef.current = setTimeout(() => {
                pendingDisplayResetRef.current = null;
                setSubmittedDisplayValue(null);
            }, 2000);
            emitInlineUpdate(nextValue);
        }
        if (!keepEditing) {
            setIsEditing(false);
        }
        return true;
    };

    const beginEditing = () => {
        if (effectiveEditingDisabled) {
            const nextBlockedNoticeAt = Date.now();
            if (
                editingDisabledReason
                && typeof onEditBlocked === 'function'
                && nextBlockedNoticeAt - lastBlockedNoticeRef.current > 400
            ) {
                lastBlockedNoticeRef.current = nextBlockedNoticeAt;
                onEditBlocked({
                    rowId: rowPath,
                    colId: column.id,
                    reason: editingDisabledReason,
                });
            }
            return;
        }
        if (!isRowEditing && supportsRowEditSession && typeof onRequestRowStart === 'function') {
            onRequestRowStart(row, {
                trigger: 'cell',
                focusColumnId: column.id,
                autoSave: true,
            });
            return;
        }
        const baseValue = rowSessionDraft !== undefined
            ? rowSessionDraft
            : (currentCellValue !== undefined && currentCellValue !== null ? currentCellValue : '');
        setValue(toEditorInputValue(baseValue, resolvedEditorConfig));
        setIsEditing(true);
        if (typeof requestEditorOptions === 'function' && resolvedEditorConfig.optionsSource) {
            requestEditorOptions(resolvedEditorConfig, column.id, columnConfig || null);
        }
    };

    const onBlur = () => {
        commitValue();
    };

    const editorChromeStyle = {
        width: '100%',
        height: '100%',
        border: error ? '2px solid #dc2626' : '2px solid #2196f3',
        borderRadius: '0',
        padding: editorType === 'checkbox' ? '0 6px' : '0 4px',
        margin: 0,
        outline: 'none',
        fontFamily: 'inherit',
        fontSize: 'inherit',
        textAlign: editorType === 'number' ? 'right' : 'left',
        background: 'transparent',
        ...CELL_CONTENT_RESET_STYLE,
    };

    const commonInputProps = {
        ref: inputRef,
        'data-edit-rowid': rowPath,
        'data-edit-colid': column.id,
        'data-editor-type': editorType,
        onBlur,
            onKeyDown: (e) => {
            if (e.key === 'Enter' && editorType !== 'textarea') {
                e.preventDefault();
                if (isRowEditing) {
                    const committed = commitValue();
                    if (
                        committed
                        && rowEditSession
                        && !rowEditSession.autoSave
                        && (e.ctrlKey || e.metaKey)
                        && typeof onRequestRowSave === 'function'
                    ) {
                        onRequestRowSave(rowPath);
                    }
                    return;
                }
                commitValue();
            }
            if (e.key === 'Escape') {
                e.preventDefault();
                if (isRowEditing && typeof onRequestRowCancel === 'function') {
                    onRequestRowCancel(rowPath);
                    return;
                }
                setIsEditing(false);
                setError(null);
                setValue(toEditorInputValue(currentCellValue !== undefined && currentCellValue !== null ? currentCellValue : '', resolvedEditorConfig));
            }
        },
        style: editorChromeStyle,
    };

    const renderEditor = () => {
        if (editorType === 'textarea') {
            return (
                <textarea
                    {...commonInputProps}
                    rows={resolvedEditorConfig.rows || 3}
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    placeholder={resolvedEditorConfig.placeholder || ''}
                    style={{
                        ...editorChromeStyle,
                        resize: 'none',
                        paddingTop: '6px',
                        paddingBottom: '6px',
                        minHeight: '100%',
                    }}
                />
            );
        }

        if (editorType === 'checkbox') {
            return (
                <label
                    style={{
                        ...editorChromeStyle,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        cursor: effectiveEditingDisabled ? 'default' : 'pointer',
                    }}
                >
                    <input
                        {...commonInputProps}
                        type="checkbox"
                        checked={Boolean(value)}
                        onChange={(e) => {
                            const nextChecked = Boolean(e.target.checked);
                            setValue(nextChecked);
                            if (isRowEditing) {
                                commitRowDraft(nextChecked);
                                return;
                            }
                            commitValue({ nextValue: nextChecked });
                        }}
                        style={{ width: '16px', height: '16px' }}
                    />
                </label>
            );
        }

        if (editorType === 'select') {
            return (
                <select
                    {...commonInputProps}
                    value={value === null || value === undefined ? '' : value}
                    onChange={(e) => {
                        const nextValue = e.target.value;
                        setValue(nextValue);
                        if (isRowEditing) {
                            commitRowDraft(coerceEditorValue(nextValue, resolvedEditorConfig, currentCellValue));
                            return;
                        }
                        if (resolvedEditorConfig.saveOnChange !== false) {
                            commitValue({ nextValue });
                        }
                    }}
                    disabled={editorOptionsLoading || Boolean(editorOptionsError)}
                    title={editorOptionsError || undefined}
                    style={{
                        ...editorChromeStyle,
                        ...(editorOptionsError ? { border: '2px solid #dc2626' } : {}),
                    }}
                >
                    {editorOptionsLoading && <option value="">Loading…</option>}
                    {editorOptionsError && <option value="">Error loading options</option>}
                    {!editorOptionsLoading && !editorOptionsError && !resolvedEditorConfig.allowCustomValue && <option value="">Select…</option>}
                    {!editorOptionsError && normalizedOptions.map((option) => (
                        <option key={`${String(option.value)}:${option.label}`} value={option.value}>
                            {option.label}
                        </option>
                    ))}
                </select>
            );
        }

        if (editorType === 'richSelect') {
            return (
                <>
                    <input
                        {...commonInputProps}
                        type="text"
                        value={value}
                        list={!editorOptionsError && normalizedOptions.length > 0 ? datalistIdRef.current : undefined}
                        onChange={(e) => setValue(e.target.value)}
                        placeholder={editorOptionsLoading ? 'Loading…' : (editorOptionsError ? 'Error loading options' : (resolvedEditorConfig.placeholder || ''))}
                        title={editorOptionsError || commonInputProps.title}
                        style={{
                            ...editorChromeStyle,
                            ...(editorOptionsError ? { border: '2px solid #dc2626' } : {}),
                        }}
                    />
                    {normalizedOptions.length > 0 && (
                        <datalist id={datalistIdRef.current}>
                            {normalizedOptions.map((option) => (
                                <option key={`${String(option.value)}:${option.label}`} value={String(option.value)}>
                                    {option.label}
                                </option>
                            ))}
                        </datalist>
                    )}
                </>
            );
        }

        return (
            <input
                {...commonInputProps}
                type={editorType === 'date' ? 'date' : (editorType === 'number' ? 'number' : 'text')}
                value={value}
                min={resolvedEditorConfig.min}
                max={resolvedEditorConfig.max}
                step={resolvedEditorConfig.step}
                onChange={(e) => setValue(e.target.value)}
                placeholder={resolvedEditorConfig.placeholder || ''}
            />
        );
    };

    if (isEditing) {
        return renderEditor();
    }

    return (
        <div
            onDoubleClick={(e) => {
                e.stopPropagation();
                beginEditing();
            }}
            onContextMenu={(e) => handleContextMenu(e, effectiveDisplayValue, column.id, row)}
            data-display-rowid={rowPath}
            data-display-colid={column.id}
            data-display-value={effectiveDisplayValue !== undefined && effectiveDisplayValue !== null ? String(effectiveDisplayValue) : ''}
            style={{
                width: '100%',
                height: '100%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: editorType === 'number' ? 'flex-end' : 'flex-start',
                paddingRight: editorType === 'number' ? '8px' : '0',
                paddingLeft: editorType === 'number' ? '0' : '8px',
                cursor: effectiveEditingDisabled ? 'default' : 'cell',
                border: error ? '1px solid red' : '1px solid transparent',
                ...CELL_CONTENT_RESET_STYLE,
            }}
            title={error || editingDisabledReason || undefined}
        >
            {getDisplayText()}
        </div>
    );
};

export default EditableCell;
