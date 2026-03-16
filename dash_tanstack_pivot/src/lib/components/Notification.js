import React from 'react';
import Icons from './Icons';

const Notification = ({ message, type, onClose }) => (
    <div style={{
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
        pointerEvents: 'auto'
    }}>
        <span style={{flex: 1}}>{message}</span>
        <span onClick={onClose} style={{
            cursor:'pointer',
            opacity: 0.7,
            display: 'flex',
            padding: '2px',
            borderRadius: '4px',
            background: 'rgba(255,255,255,0.1)'
        }}><Icons.Close/></span>
    </div>
);

export default Notification;
