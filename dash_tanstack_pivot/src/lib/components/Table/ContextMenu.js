import React, { useState, useEffect, useLayoutEffect, useRef } from 'react';

const ContextMenu = ({ x, y, onClose, actions, theme }) => {
    const [adjustedPosition, setAdjustedPosition] = useState({ x, y });
    const menuRef = useRef(null);

    useLayoutEffect(() => {
        const clampPosition = () => {
            const viewportWidth = window.innerWidth;
            const viewportHeight = window.innerHeight;
            const rect = menuRef.current ? menuRef.current.getBoundingClientRect() : null;
            const menuWidth = rect ? rect.width : 200;
            const menuHeight = rect ? rect.height : (actions.length * 32 + 20);
            const padding = 8;

            let adjustedX = x;
            let adjustedY = y;

            if (adjustedX + menuWidth > viewportWidth - padding) {
                adjustedX = viewportWidth - menuWidth - padding;
            }
            if (adjustedY + menuHeight > viewportHeight - padding) {
                adjustedY = viewportHeight - menuHeight - padding;
            }

            adjustedX = Math.max(padding, adjustedX);
            adjustedY = Math.max(padding, adjustedY);

            setAdjustedPosition({ x: adjustedX, y: adjustedY });
        };

        clampPosition();
        window.addEventListener('resize', clampPosition);
        window.addEventListener('scroll', clampPosition, true);

        return () => {
            window.removeEventListener('resize', clampPosition);
            window.removeEventListener('scroll', clampPosition, true);
        };
    }, [x, y, actions]);

    useEffect(() => {
        setAdjustedPosition({ x, y });
    }, [x, y]);

    return (
        <div ref={menuRef} style={{
            position: 'fixed',
            top: adjustedPosition.y,
            left: adjustedPosition.x,
            background: theme?.surfaceBg || theme?.background || '#fff',
            border: `1px solid ${theme?.border || '#ccc'}`,
            boxShadow: theme?.shadowMd || '0 4px 20px rgba(0,0,0,0.15)',
            zIndex: 10001, // Above notifications
            padding: '6px 0',
            borderRadius: theme?.radiusSm || '8px',
            fontSize: '13px',
            minWidth: '180px',
            maxWidth: '300px',
            color: theme?.text || '#111'
        }}>
            {actions.map((action, i) => {
                if (action === 'separator') {
                    return <div key={i} style={{height: '1px', background: theme?.border || '#e0e0e0', margin: '4px 0'}} />;
                }
                return (
                    <div key={i} onClick={() => { action.onClick(); onClose(); }} style={{
                        padding: '8px 16px',
                        cursor: 'pointer',
                        backgroundColor: theme?.surfaceBg || theme?.background || '#fff',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        color: theme?.text || '#111'
                    }} onMouseEnter={e => e.currentTarget.style.backgroundColor = theme?.hover || '#f5f5f5'} onMouseLeave={e => e.currentTarget.style.backgroundColor = theme?.surfaceBg || theme?.background || '#fff'}>
                        {action.icon && <span style={{color: theme?.textSec || '#757575'}}>{action.icon}</span>}
                        {action.label}
                    </div>
                );
            })}
        </div>
    );
};

export default ContextMenu;
