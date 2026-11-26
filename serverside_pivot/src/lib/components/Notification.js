import React from 'react';
import Icons from '../utils/Icons';

/** @param {any} p0 @returns {any} */
const Notification = ({ message, type, onClose }) => {
    const accentColor = type === 'error'
        ? 'var(--sp-danger, #ef4444)'
        : type === 'warning'
            ? 'var(--sp-warning, #f59e0b)'
            : type === 'success'
                ? 'var(--sp-success, #10b981)'
                : 'var(--sp-primary, #4F46E5)';
    return (
        <div role="status" aria-live="polite" className="sp-toast-surface" style={{
            // Centred with auto margins rather than a transform, so the enter
            // animation owns transform without having to repeat the centring.
            position: 'fixed',
            bottom: '20px',
            left: 0,
            right: 0,
            marginLeft: 'auto',
            marginRight: 'auto',
            width: 'max-content',
            padding: '10px 14px 10px 16px',
            borderRadius: 'var(--sp-radius-pill, 999px)',
            color: 'var(--sp-text, #f8fafc)',
            fontSize: 'var(--sp-fs-base, 13px)',
            fontWeight: 500,
            fontFamily: 'var(--sp-font-sans, system-ui, -apple-system, sans-serif)',
            background: 'var(--sp-glass-bg, rgba(28, 28, 30, 0.90))',
            backdropFilter: 'blur(18px) saturate(180%)',
            WebkitBackdropFilter: 'blur(18px) saturate(180%)',
            border: '1px solid var(--sp-glass-border, rgba(255, 255, 255, 0.14))',
            boxShadow: 'var(--sp-shadow, 0 12px 32px rgba(0,0,0,0.28))',
            animation: 'pivot-toast-enter var(--sp-dur-base, 200ms) var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) both',
            zIndex: 10000,
            display: 'flex',
            alignItems: 'center',
            gap: '10px',
            maxWidth: 'min(440px, calc(100vw - 32px))',
            minWidth: '220px',
            // Feedback can overlap a cell near the viewport bottom. Keep that
            // cell usable while the notification is visible; only dismiss targets input.
            pointerEvents: 'none'
        }}>
            <span aria-hidden="true" style={{
                width: '8px',
                height: '8px',
                borderRadius: '999px',
                background: accentColor,
                boxShadow: `0 0 0 3px ${type === 'error' ? 'rgba(239,68,68,0.16)' : type === 'warning' ? 'rgba(245,158,11,0.16)' : type === 'success' ? 'rgba(16,185,129,0.16)' : 'var(--sp-focus-ring, rgba(79,70,229,0.18))'}`,
                flexShrink: 0,
            }} />
            <span style={{ flex: 1, lineHeight: 1.35 }}>{message}</span>
            <button type="button" className="sp-btn sp-btn-ghost" aria-label="Dismiss notification" onClick={onClose} style={{
                pointerEvents: 'auto',
                border: 0,
                margin: 0,
                color: 'var(--sp-text-sec, inherit)',
                font: 'inherit',
                cursor: 'pointer',
                opacity: 0.78,
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '24px',
                height: '24px',
                padding: 0,
                borderRadius: '999px',
                background: 'var(--sp-hover, rgba(255,255,255,0.1))'
            }}><Icons.Close/></button>
        </div>
    );
};

export default Notification;
