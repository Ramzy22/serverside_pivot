declare module 'echarts-gl/lib/export/charts' {
    export const Bar3DChart: any;
    export const Line3DChart: any;
    export const Scatter3DChart: any;
    export const SurfaceChart: any;
}

declare module 'echarts-gl/lib/export/components' {
    export const Grid3DComponent: any;
}

declare module 'react-dom' {
    import type * as React from 'react';
    export function createPortal(
        children: React.ReactNode,
        container: Element | DocumentFragment,
        key?: null | string,
    ): React.ReactPortal;
    export function render(
        element: React.ReactElement,
        container: Element | DocumentFragment | null,
        callback?: () => void,
    ): any;
    export function unmountComponentAtNode(container: Element | DocumentFragment): boolean;
    export function flushSync<R>(fn: () => R): R;
    const ReactDOM: {
        createPortal: typeof createPortal;
        render: typeof render;
        unmountComponentAtNode: typeof unmountComponentAtNode;
        flushSync: typeof flushSync;
        [key: string]: any;
    };
    export default ReactDOM;
}

declare module 'file-saver' {
    export function saveAs(data: Blob | string, filename?: string, options?: any): void;
    const FileSaver: {
        saveAs: typeof saveAs;
        [key: string]: any;
    };
    export default FileSaver;
}

declare module 'xlsx/dist/xlsx.mini.min.js' {
    const XLSX: any;
    export = XLSX;
}

interface Window {
    __PIVOT_DEBUG__?: boolean;
    __PIVOT_PROFILE__?: boolean;
    __PIVOT_PROFILE_CONSOLE__?: boolean;
    __pivotProfiler?: any;
    __PIVOT_QUALIFY_KEYBOARD_MOVES__?: boolean;
    __pivotKeyboardMoveTrace?: any[];
}
