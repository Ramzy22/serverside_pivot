import { useEffect, useRef } from 'react';
import { getPivotProfiler, isPivotProfilingEnabled } from '../utils/pivotProfiler';

/** @param {any} [componentName] @param {any} [componentId] @returns {any} */
export function usePivotRenderCounter(componentName, componentId = null) {
    /** @type {any} */
    const renderCountRef = useRef(0);
    renderCountRef.current += 1;

    useEffect(() => {
        if (!isPivotProfilingEnabled()) return;
        /** @type {any} */
        const profiler = getPivotProfiler();
        if (!profiler || typeof profiler.render !== 'function') return;
        profiler.render({
            componentName,
            componentId,
            count: renderCountRef.current,
            renderedAt: Date.now(),
        });
    });

    return renderCountRef.current;
}
