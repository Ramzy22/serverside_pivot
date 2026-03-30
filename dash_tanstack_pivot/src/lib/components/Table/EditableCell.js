import React, { useState, useEffect, useRef } from 'react';
import { formatValue } from '../../utils/helpers';

const EditableCell = ({ 
    getValue, 
    row, 
    column, 
    format, 
    numberGroupSeparator,
    validationRules,
    setProps,
    handleContextMenu
}) => {
    const initialValue = getValue();
    const [value, setValue] = useState(initialValue);
    const [isEditing, setIsEditing] = useState(false);
    const [error, setError] = useState(null);
    const inputRef = useRef(null);

    useEffect(() => {
        setValue(initialValue);
    }, [initialValue]);

    useEffect(() => {
        if (isEditing && inputRef.current) {
            inputRef.current.focus();
            inputRef.current.select();
        }
    }, [isEditing]);

    const validate = (val) => {
        if (!validationRules || !validationRules[column.id]) return true;
        const rules = validationRules[column.id];
        for (const rule of rules) {
            if (rule.type === 'required' && (val === null || val === '')) return false;
            if (rule.type === 'numeric' && isNaN(Number(val))) return false;
            if (rule.type === 'min' && Number(val) < rule.value) return false;
            if (rule.type === 'max' && Number(val) > rule.value) return false;
            if (rule.type === 'regex' && !new RegExp(rule.pattern).test(val)) return false;
        }
        return true;
    };

    const onBlur = () => {
        setIsEditing(false);
        setError(null);
        
        // Basic type conversion for numeric fields
        let submitValue = value;
        // Check if the column is generally numeric (based on format or current value)
        const isNumeric = typeof initialValue === 'number' || (format && (format.startsWith('fixed') || format === 'currency' || format === 'percent'));
        
        if (isNumeric && value !== '') {
             submitValue = Number(value);
        }

        if (String(submitValue) !== String(initialValue)) {
            if (validate(submitValue)) {
                 if (setProps) {
                    setProps({
                        cellUpdate: {
                            rowId: row.id,
                            colId: column.id,
                            value: submitValue,
                            oldValue: initialValue,
                            timestamp: Date.now()
                        }
                    });
                }
            } else {
                setError("Invalid value");
                // Optional: keep editing or revert. For now, revert after short delay or keep visual error
                console.warn("Validation failed for", submitValue);
                setValue(initialValue); 
            }
        }
    };

    return isEditing ? (
        <input 
            ref={inputRef}
            value={value} 
            onChange={e => setValue(e.target.value)} 
            onBlur={onBlur}
            onKeyDown={e => {
                if(e.key === 'Enter') {
                    e.preventDefault();
                    onBlur();
                }
                if(e.key === 'Escape') {
                    setIsEditing(false);
                    setValue(initialValue);
                }
                if(e.key === 'Tab') {
                     // Tab handling is complex in React Table without custom logic, relying on default behavior for now (blur)
                }
            }}
            style={{
                width: '100%', 
                height: '100%', 
                border: error ? '2px solid red' : '2px solid #2196f3',
                borderRadius: '0',
                padding: '0 4px',
                margin: 0,
                outline: 'none',
                fontFamily: 'inherit',
                fontSize: 'inherit',
                textAlign: 'right' 
            }}
        />
    ) : (
        <div 
            onDoubleClick={() => setIsEditing(true)}
            onContextMenu={e => handleContextMenu(e, initialValue, column.id, row)}
            style={{
                width: '100%', 
                height: '100%', 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'flex-end', 
                paddingRight: '8px',
                cursor: 'cell',
                border: error ? '1px solid red' : '1px solid transparent'
            }}
            title={error || undefined}
        >
            {formatValue(initialValue, format, undefined, numberGroupSeparator)}
        </div>
    );
};

export default EditableCell;
