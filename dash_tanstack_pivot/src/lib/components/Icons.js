import React from 'react';

const Icons = {
    SortAsc: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M4 12l1.41 1.41L11 7.83V20h2V7.83l5.58 5.59L20 12l-8-8-8 8z"/></svg>,
    SortDesc: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M20 12l-1.41-1.41L13 16.17V4h-2v12.17l-5.58-5.59L4 12l8 8 8-8z"/></svg>,
    Export: () => <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg>,
    Search: () => <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M15.5 14h-.79l-.28-.27C15.41 12.59 16 11.11 16 9.5 16 5.91 13.09 3 9.5 3S3 5.91 3 9.5 5.91 16 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>,
    ChevronRight: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"/></svg>,
    ChevronDown: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M16.59 8.59L12 13.17 7.41 8.59 6 10l6 6 6-6z"/></svg>,
    DragIndicator: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" style={{color:'#9CA3AF', flexShrink:0}}><path d="M11 18c0 1.1-.9 2-2 2s-2-.9-2-2 .9-2 2-2 2 .9 2 2zm-2-8c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0-6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm6 4c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"/></svg>,
    // Section header icons
    List: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M4 6h16v2H4zm0 5h16v2H4zm0 5h16v2H4z"/></svg>,
    Columns: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M10 18h5V5h-5v13zm-6 0h5V5H4v13zM16 5v13h4V5h-4z"/></svg>,
    Sigma: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M18 6h-8.74l5.26 6-5.26 6H18v2H5v-2l6-6.87L5 4V2h13v4z"/></svg>,
    Database: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 4.02 2 6.5v11C2 19.98 6.48 22 12 22s10-2.02 10-4.5v-11C22 4.02 17.52 2 12 2zm0 2c4.42 0 8 1.57 8 3.5S16.42 9 12 9 4 7.43 4 6.5 7.58 4 12 4zm0 16c-4.42 0-8-1.57-8-3.5v-2.05c1.77 1.28 4.71 2.05 8 2.05s6.23-.77 8-2.05V16.5c0 1.93-3.58 3.5-8 3.5zm0-5c-4.42 0-8-1.57-8-3.5V9.45c1.77 1.28 4.71 2.05 8 2.05s6.23-.77 8-2.05V11.5c0 1.93-3.58 3.5-8 3.5z"/></svg>,
    Close: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>,
    Spacing: () => <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z"/></svg>,
    Transpose: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M4 7h10.17l-2.58-2.59L13 3l5 5-5 5-1.41-1.41L14.17 9H4V7zm16 10H9.83l2.58 2.59L11 21l-5-5 5-5 1.41 1.41L9.83 15H20v2z"/></svg>,
    ColExpand: () => <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z"/></svg>,
    ColCollapse: () => <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M19 13H5v-2h14v2z"/></svg>,
    Filter: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M10 18h4v-2h-4v2zM3 6v2h18V6H3zm3 7h12v-2H6v2z"/></svg>,
    Menu: () => <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z"/></svg>,
    MoreVert: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"/></svg>,
    DataBars: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M4 19h16v1H4v-1zm2-2h3V9H6v8zm5 0h3V5h-3v12zm5 0h3v-6h-3v6z"/></svg>,
    PinLeft: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M13 12H7v1.5L4.5 11 7 8.5V10h6v2zm6.41-7.12l-1.42-1.41-4.83 4.83c-.37-.13-.77-.21-1.19-.21-1.91 0-3.47 1.55-3.47 3.47 0 1.92 1.56 3.47 3.47 3.47 1.92 0 3.47-1.55 3.47-3.47 0-.42-.08-.82-.21-1.19l4.83-4.83-1.42-1.41z"/></svg>,
    PinRight: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M11 12h6v-1.5l2.5 2.5-2.5 2.5V14h-6v-2zm-6.41 7.12l1.42 1.41 4.83-4.83c.37.13.77.21 1.19.21 1.91 0 3.47-1.55 3.47-3.47 0-1.92-1.56-3.47-3.47-3.47-1.92 0-3.47 1.55-3.47 3.47 0 .42.08.82.21 1.19l-4.83 4.83 1.42 1.41z"/></svg>,
    Unpin: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M16 9V4l1 1V3H7v1l1-1v5c0 1.66-1.34 3-3 3v2h5.97v7l1 1 1-1v-7H19v-2c-1.66 0-3-1.34-3-3z"/></svg>,
    Save: () => <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><path d="M17 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V7l-4-4zm2 16H5V5h11.17L19 7.83V19zm-7-7a3 3 0 1 0 0 6 3 3 0 0 0 0-6zm-5-5h8v4H7V7z"/></svg>,
    Visibility: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg>,
    VisibilityOff: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12 7c2.76 0 5 2.24 5 5 0 .65-.13 1.26-.36 1.82l2.92 2.92c1.51-1.39 2.72-3.13 3.44-5.04-1.73-4.39-6-7.5-11-7.5-1.4 0-2.74.25-3.98.7l2.16 2.16C10.74 7.13 11.35 7 12 7zM2 4.27l2.28 2.28.46.46C3.08 8.3 1.78 10.02 1 12c1.73 4.39 6 7.5 11 7.5 1.55 0 3.03-.3 4.38-.84l.42.42L19.73 22 21 20.73 3.27 3 2 4.27zM7.53 9.8l1.55 1.55c-.05.21-.08.43-.08.65 0 1.66 1.34 3 3 3 .22 0 .44-.03.65-.08l1.55 1.55c-.67.33-1.41.53-2.2.53-2.76 0-5-2.24-5-5 0-.79.2-1.53.53-2.2zm4.31-.78l3.15 3.15.02-.17c0-1.66-1.34-3-3-3l-.17.02z"/></svg>,
    Group: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5s-3 1.34-3 3 1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5 5 1.34 5 3 6.34 3 8 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z"/></svg>,
    Lock: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zm-6 9c-1.1 0-2-.9-2-2s.9-2 2-2 2 .9 2 2-.9-2-2 2zm3.1-9H8.9V6c0-1.71 1.39-3.1 3.1-3.1 1.71 0 3.1 1.39 3.1 3.1v2z"/></svg>
};

export default Icons;
