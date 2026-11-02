import React from 'react';
import Icons from '../utils/Icons';

const Notification = ({ message, type, onClose }) => (
    <div role="status" aria-live="polite" style={{
        position: 'fixed', // Changed from absolute
        bottom: '20px',
        left: '50%',
        transform: 'translateX(-50%)',
        padding: '12px 20px',
        borderRadius: '8px',
        color: '#fff',
        fontSize: '14px',
        background: type === 'error' ? '#d32f2f' :
                   type === 'warning' ? '#f57c00' : '#323232',
        boxShadow: '0 4px 12px rgba(0,0,0,0.25)',
        zIndex: 10000, // High z-index
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        maxWidth: '400px',
        minWidth: '200px',
        // Feedback can overlap a cell near the viewport bottom. Keep that
        // cell usable while the notification is visible; only dismiss targets input.
        pointerEvents: 'none'
    }}>
        <span style={{flex: 1}}>{message}</span>
        <button type="button" aria-label="Dismiss notification" onClick={onClose} style={{
            pointerEvents: 'auto',
            border: 0,
            margin: 0,
            color: 'inherit',
            font: 'inherit',
            cursor:'pointer',
            opacity: 0.7,
            display: 'flex',
            padding: '2px',
            borderRadius: '4px',
            background: 'rgba(255,255,255,0.1)'
        }}><Icons.Close/></button>
    </div>
);

export default Notification;
