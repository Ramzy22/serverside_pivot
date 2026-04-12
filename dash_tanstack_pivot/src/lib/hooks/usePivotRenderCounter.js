import { useEffect, useRef } from 'react';
import { getPivotProfiler, isPivotProfilingEnabled } from '../utils/pivotProfiler';

export function usePivotRenderCounter(componentName, componentId = null) {
    const renderCountRef = useRef(0);
    renderCountRef.current += 1;

    useEffect(() => {
        if (!isPivotProfilingEnabled()) return;
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
