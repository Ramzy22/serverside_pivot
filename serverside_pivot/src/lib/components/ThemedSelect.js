import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import ReactDOM from 'react-dom';
import Icons from '../utils/Icons';
import { PivotThemeContext } from '../contexts/PivotThemeContext';

/** @param {any} children @param {any} [groupLabel] @returns {any[]} */
const flattenSelectChildren = (children, groupLabel = null) => {
    /** @type {any[]} */
    const items = [];
    React.Children.forEach(children, (child) => {
        if (!React.isValidElement(child)) return;
        const typeName = typeof child.type === 'string' ? child.type.toLowerCase() : '';
        /** @type {any} */
        const props = child.props || {};
        if (typeName === 'optgroup') {
            const label = props.label || '';
            items.push(...flattenSelectChildren(props.children, label));
            return;
        }
        if (typeName === 'option') {
            const val = props.value !== undefined ? String(props.value) : String(props.children ?? '');
            const text = typeof props.children === 'string' || typeof props.children === 'number'
                ? String(props.children)
                : (Array.isArray(props.children) ? props.children.join('') : String(props.children ?? val));
            items.push({
                value: val,
                label: text,
                disabled: Boolean(props.disabled),
                group: groupLabel,
                title: props.title,
            });
        }
    });
    return items;
};

/**
 * Accessible custom themed dropdown replacing native <select> elements.
 * Supports both an `options` array and standard JSX `<option>`/`<optgroup>` children,
 * emitting a synthetic `{ target: { value }, currentTarget: { value } }` event to `onChange`.
 * @type {any}
 */
const ThemedSelect = React.forwardRef(/** @param {any} p0 @param {any} forwardedRef @returns {any} */ function ThemedSelect({
    value,
    defaultValue,
    onChange,
    onClick,
    onMouseDown,
    onKeyDown,
    onBlur,
    options,
    children,
    disabled = false,
    title,
    style = {},
    menuStyle = {},
    className = '',
    placeholder = 'Select...',
    size = 'md',
    portalTarget = null,
    id,
    name,
    'aria-label': ariaLabel,
    ...restProps
}, forwardedRef) {
    /** @type {any} */
    const themeCtx = React.useContext(PivotThemeContext);
    /** @type {any} */
    const theme = themeCtx?.theme || null;
    const isDark = Boolean(theme?.isDark);
    /** @type {any} */
    const triggerRef = useRef(null);
    /** @type {any} */
    const setTriggerRef = useCallback(/** @param {any} node */ (node) => {
        triggerRef.current = node;
        if (typeof forwardedRef === 'function') forwardedRef(node);
        else if (forwardedRef && typeof forwardedRef === 'object') forwardedRef.current = node;
    }, [forwardedRef]);
    /** @type {any} */
    const menuRef = useRef(null);
    /** @type {any} */
    const typeaheadBufferRef = useRef('');
    /** @type {any} */
    const typeaheadTimerRef = useRef(null);
    const reactId = typeof React.useId === 'function' ? React.useId() : 'sp-select';
    const listboxId = `${id || reactId}-listbox`;
    const [open, setOpen] = useState(false);
    const [coords, setCoords] = useState({ top: 0, left: 0, minWidth: 120, maxHeight: 280 });
    const [internalValue, setInternalValue] = useState(defaultValue !== undefined ? String(defaultValue) : '');

    useEffect(() => () => {
        if (typeaheadTimerRef.current) clearTimeout(typeaheadTimerRef.current);
    }, []);

    /** @type {any[]} */
    const parsedOptions = useMemo(() => {
        if (Array.isArray(options)) {
            return options.map(/** @param {any} opt */ (opt) => (
                typeof opt === 'object' && opt !== null
                    ? {
                        value: String(opt.value ?? ''),
                        label: String(opt.label ?? opt.value ?? ''),
                        disabled: Boolean(opt.disabled),
                        group: opt.group || null,
                        title: opt.title,
                    }
                    : { value: String(opt), label: String(opt), disabled: false, group: null }
            ));
        }
        return flattenSelectChildren(children);
    }, [options, children]);

    const currentValue = value !== undefined && value !== null ? String(value) : internalValue;
    const selectedOption = parsedOptions.find(/** @param {any} opt */ (opt) => opt.value === currentValue) || null;
    const [highlightedIndex, setHighlightedIndex] = useState(() => {
        const idx = parsedOptions.findIndex(/** @param {any} opt */ (opt) => opt.value === currentValue && !opt.disabled);
        return idx >= 0 ? idx : 0;
    });

    const updateMenuPosition = useCallback(() => {
        if (!triggerRef.current || typeof window === 'undefined') return;
        const rect = triggerRef.current.getBoundingClientRect();
        const viewportH = window.innerHeight || 768;
        const viewportW = window.innerWidth || 1024;
        const estimatedHeight = Math.min(280, Math.max(44, parsedOptions.length * 30 + 12));
        const spaceBelow = viewportH - rect.bottom - 8;
        const spaceAbove = rect.top - 8;
        const openAbove = spaceBelow < estimatedHeight && spaceAbove > spaceBelow;
        const maxHeight = Math.max(110, Math.min(300, openAbove ? spaceAbove : spaceBelow));
        const minWidth = Math.max(rect.width || 110, 128);
        const left = Math.max(8, Math.min(rect.left, viewportW - minWidth - 8));
        const top = openAbove ? Math.max(8, rect.top - Math.min(estimatedHeight, maxHeight) - 4) : (rect.bottom + 4);
        setCoords({ top, left, minWidth, maxHeight });
    }, [parsedOptions.length]);

    useEffect(() => {
        if (!open) return undefined;
        updateMenuPosition();
        const idx = parsedOptions.findIndex(/** @param {any} opt */ (opt) => opt.value === currentValue && !opt.disabled);
        setHighlightedIndex(idx >= 0 ? idx : parsedOptions.findIndex(/** @param {any} opt */ (opt) => !opt.disabled));
        /** @param {any} event */
        const handleOutside = (event) => {
            const path = typeof event.composedPath === 'function' ? event.composedPath() : null;
            if (triggerRef.current && (triggerRef.current.contains(event.target) || (path && path.includes(triggerRef.current)))) return;
            if (menuRef.current && (menuRef.current.contains(event.target) || (path && path.includes(menuRef.current)))) return;
            setOpen(false);
        };
        /** @param {any} [event] */
        const handleViewportChange = (event) => {
            const path = event && typeof event.composedPath === 'function' ? event.composedPath() : null;
            if (menuRef.current && ((event && event.target && typeof menuRef.current.contains === 'function' && menuRef.current.contains(event.target)) || (path && path.includes(menuRef.current)))) {
                return;
            }
            updateMenuPosition();
        };
        document.addEventListener('mousedown', handleOutside, true);
        window.addEventListener('resize', handleViewportChange);
        window.addEventListener('scroll', handleViewportChange, true);
        return () => {
            document.removeEventListener('mousedown', handleOutside, true);
            window.removeEventListener('resize', handleViewportChange);
            window.removeEventListener('scroll', handleViewportChange, true);
        };
    }, [open, updateMenuPosition, parsedOptions, currentValue]);

    useEffect(() => {
        if (!open || highlightedIndex < 0 || !menuRef.current) return;
        const activeOptionEl = typeof menuRef.current.querySelector === 'function'
            ? menuRef.current.querySelector('[data-highlighted="true"]')
            : null;
        if (activeOptionEl && typeof activeOptionEl.scrollIntoView === 'function') {
            activeOptionEl.scrollIntoView({ block: 'nearest' });
        }
    }, [open, highlightedIndex]);

    const selectValue = useCallback(/** @param {string} nextVal */ (nextVal) => {
        if (value === undefined) setInternalValue(nextVal);
        setOpen(false);
        if (onChange) {
            onChange({
                target: { value: nextVal, name, id },
                currentTarget: { value: nextVal, name, id },
                preventDefault: () => {},
                stopPropagation: () => {},
            });
        }
        requestAnimationFrame(() => {
            triggerRef.current?.focus?.();
        });
    }, [value, onChange, name, id]);

    /** @param {any} event */
    const handleTriggerKeyDown = (event) => {
        if (disabled) return;
        if (open && (event.key === 'Escape' || event.key === 'Enter')) {
            event.preventDefault();
            event.stopPropagation();
            if (event.key === 'Escape') {
                setOpen(false);
            } else if (parsedOptions[highlightedIndex] && !parsedOptions[highlightedIndex].disabled) {
                selectValue(parsedOptions[highlightedIndex].value);
            }
            return;
        }
        if (onKeyDown) onKeyDown(event);
        if (event.defaultPrevented) return;
        if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault();
            if (!open) {
                setOpen(true);
                return;
            }
            const step = event.key === 'ArrowDown' ? 1 : -1;
            const len = parsedOptions.length;
            if (len === 0) return;
            let nextIdx = highlightedIndex;
            for (let i = 0; i < len; i++) {
                nextIdx = (nextIdx + step + len) % len;
                if (!parsedOptions[nextIdx].disabled) {
                    setHighlightedIndex(nextIdx);
                    break;
                }
            }
        } else if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            if (!open) {
                setOpen(true);
            } else if (parsedOptions[highlightedIndex] && !parsedOptions[highlightedIndex].disabled) {
                selectValue(parsedOptions[highlightedIndex].value);
            }
        } else if (event.key === 'Tab' && open) {
            setOpen(false);
        } else if (
            typeof event.key === 'string'
            && event.key.length === 1
            && event.key !== ' '
            && !event.ctrlKey
            && !event.metaKey
            && !event.altKey
            && parsedOptions.length > 0
        ) {
            event.preventDefault();
            if (typeaheadTimerRef.current) clearTimeout(typeaheadTimerRef.current);
            const nextQuery = (typeaheadBufferRef.current + event.key).toLowerCase();
            typeaheadBufferRef.current = nextQuery;
            typeaheadTimerRef.current = setTimeout(() => {
                typeaheadBufferRef.current = '';
                typeaheadTimerRef.current = null;
            }, 500);
            const len = parsedOptions.length;
            const startIdx = Math.max(0, highlightedIndex);
            for (let offset = 0; offset < len; offset++) {
                const candidateIdx = (startIdx + (nextQuery.length === 1 ? offset + 1 : offset)) % len;
                const candidate = parsedOptions[candidateIdx];
                if (candidate && !candidate.disabled && String(candidate.label || '').toLowerCase().startsWith(nextQuery)) {
                    setHighlightedIndex(candidateIdx);
                    if (!open) selectValue(candidate.value);
                    break;
                }
            }
        }
    };

    const surfaceBg = theme?.surface || (isDark ? '#181E29' : '#FFFFFF');
    const triggerBg = style?.background || style?.backgroundColor || theme?.headerSubtleBg || theme?.inputBg || (isDark ? 'rgba(255,255,255,0.06)' : '#F8FAFC');
    const borderColor = style?.borderColor || theme?.border || (isDark ? 'rgba(255,255,255,0.14)' : 'rgba(15,23,42,0.14)');
    const textColor = style?.color || theme?.text || (isDark ? '#F1F5F9' : '#0F172A');
    const mutedColor = theme?.textSec || (isDark ? '#94A3B8' : '#64748B');
    const primaryColor = theme?.primary || '#3B82F6';
    const hoverBg = theme?.hover || (isDark ? 'rgba(255,255,255,0.08)' : 'rgba(15,23,42,0.06)');
    const selectedBg = theme?.select || (isDark ? 'rgba(59,130,246,0.18)' : 'rgba(59,130,246,0.12)');
    const compact = size === 'sm' || (style && style.fontSize && parseInt(String(style.fontSize), 10) <= 11);

    /** @type {any[]} */
    const renderedGroups = useMemo(() => {
        /** @type {any[]} */
        const result = [];
        /** @type {any} */
        let lastGroup = null;
        parsedOptions.forEach(/** @param {any} opt @param {number} idx */ (opt, idx) => {
            if (opt.group && opt.group !== lastGroup) {
                lastGroup = opt.group;
                result.push({ kind: 'group', label: opt.group, key: `grp-${idx}-${opt.group}` });
            }
            result.push({ kind: 'option', opt, idx, key: `opt-${idx}-${opt.value}` });
        });
        return result;
    }, [parsedOptions]);

    const menuNode = open ? (
        <div
            ref={menuRef}
            id={listboxId}
            role="listbox"
            aria-label={ariaLabel || title || 'Options'}
            className="sp-menu-surface"
            onMouseDown={(e) => {
                // Keep focus on the trigger button when clicking scrollbar or group headers
                e.preventDefault();
            }}
            style={{
                position: 'fixed',
                top: `${coords.top}px`,
                left: `${coords.left}px`,
                minWidth: `${coords.minWidth}px`,
                maxWidth: '320px',
                maxHeight: `${coords.maxHeight}px`,
                overflowY: 'auto',
                zIndex: 10050,
                background: surfaceBg,
                color: textColor,
                colorScheme: isDark ? 'dark' : 'light',
                border: `1px solid ${borderColor}`,
                borderRadius: '10px',
                padding: '4px',
                boxShadow: isDark
                    ? '0 16px 36px rgba(0,0,0,0.52), 0 0 0 1px rgba(255,255,255,0.06)'
                    : '0 14px 32px rgba(15,23,42,0.14), 0 0 0 1px rgba(15,23,42,0.04)',
                backdropFilter: 'blur(16px)',
                ...menuStyle,
            }}
        >
            {renderedGroups.map(/** @param {any} entry */ (entry) => {
                if (entry.kind === 'group') {
                    return (
                        <div
                            key={entry.key}
                            style={{
                                fontSize: '10px',
                                fontWeight: 700,
                                textTransform: 'uppercase',
                                letterSpacing: '0.4px',
                                color: mutedColor,
                                padding: '6px 8px 3px',
                                pointerEvents: 'none',
                            }}
                        >
                            {entry.label}
                        </div>
                    );
                }
                const { opt, idx } = entry;
                const isSelected = opt.value === currentValue;
                const isHighlighted = idx === highlightedIndex;
                return (
                    <button
                        key={entry.key}
                        id={`${listboxId}-opt-${idx}`}
                        type="button"
                        role="option"
                        aria-selected={isSelected}
                        data-value={opt.value}
                        data-highlighted={isHighlighted ? 'true' : undefined}
                        disabled={opt.disabled}
                        title={opt.title}
                        className="sp-menu-item"
                        onMouseEnter={() => setHighlightedIndex(idx)}
                        onMouseDown={(e) => {
                            e.preventDefault();
                            if (!opt.disabled) selectValue(opt.value);
                        }}
                        style={{
                            width: '100%',
                            border: 'none',
                            background: isSelected ? selectedBg : isHighlighted ? hoverBg : 'transparent',
                            color: opt.disabled ? mutedColor : (isSelected ? primaryColor : textColor),
                            fontWeight: isSelected ? 600 : 500,
                            fontSize: compact ? '11.5px' : '12.5px',
                            padding: '6px 8px',
                            borderRadius: '6px',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                            gap: '8px',
                            textAlign: 'left',
                            cursor: opt.disabled ? 'not-allowed' : 'pointer',
                            opacity: opt.disabled ? 0.45 : 1,
                        }}
                    >
                        <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{opt.label}</span>
                        {isSelected ? <span style={{ display: 'inline-flex', flexShrink: 0 }}><Icons.Check /></span> : null}
                    </button>
                );
            })}
        </div>
    ) : null;

    const targetEl = portalTarget || (typeof document !== 'undefined' ? (triggerRef.current?.closest?.('[data-pivot-workspace-root]') || document.body) : null);

    return (
        <>
            <button
                {...restProps}
                ref={setTriggerRef}
                id={id}
                type="button"
                role="combobox"
                aria-expanded={open}
                aria-haspopup="listbox"
                aria-controls={open ? listboxId : undefined}
                aria-activedescendant={open && highlightedIndex >= 0 ? `${listboxId}-opt-${highlightedIndex}` : undefined}
                aria-label={ariaLabel || title}
                value={currentValue}
                data-value={currentValue}
                title={title}
                disabled={disabled}
                className={`sp-btn sp-btn-subtle ${className}`.trim()}
                onMouseDown={(e) => {
                    if (onMouseDown) onMouseDown(e);
                }}
                onClick={(e) => {
                    if (onClick) onClick(e);
                    if (!disabled) setOpen((prev) => !prev);
                }}
                onKeyDown={handleTriggerKeyDown}
                onBlur={(e) => {
                    if (onBlur) onBlur(e);
                }}
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    gap: '6px',
                    background: triggerBg,
                    color: textColor,
                    border: style?.border || `1px solid ${borderColor}`,
                    borderRadius: style?.borderRadius || '7px',
                    padding: style?.padding || (compact ? '3px 7px' : '4px 8px'),
                    fontSize: style?.fontSize || (compact ? '11px' : '12px'),
                    fontWeight: style?.fontWeight || 500,
                    lineHeight: '1.3',
                    cursor: disabled ? 'not-allowed' : 'pointer',
                    opacity: disabled ? 0.5 : 1,
                    minHeight: style?.minHeight || (compact ? '24px' : '28px'),
                    textAlign: 'left',
                    outline: 'none',
                    ...style,
                    borderColor: open ? primaryColor : borderColor,
                    boxShadow: open ? `0 0 0 2px ${isDark ? 'rgba(59,130,246,0.28)' : 'rgba(59,130,246,0.2)'}` : style?.boxShadow,
                }}
            >
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
                    {selectedOption ? selectedOption.label : placeholder}
                </span>
                <span
                    aria-hidden="true"
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        flexShrink: 0,
                        opacity: 0.72,
                        transform: open ? 'rotate(180deg)' : 'rotate(0deg)',
                        transition: 'transform 150ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))',
                    }}
                >
                    <Icons.ChevronDown />
                </span>
            </button>
            {menuNode && targetEl ? ReactDOM.createPortal(menuNode, targetEl) : menuNode}
        </>
    );
});

export default ThemedSelect;
