import React from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { LegendComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { Bar3DChart, Line3DChart, Scatter3DChart } from 'echarts-gl/lib/export/charts';
import { Grid3DComponent } from 'echarts-gl/lib/export/components';

// Do not import the default echarts-for-react entry or `echarts-gl` barrel:
// both register every available renderer. The opt-in 3D path needs only the
// three supported series, grid3D, tooltip/legend, and Canvas rendering.
echarts.use([
    Bar3DChart,
    Line3DChart,
    Scatter3DChart,
    Grid3DComponent,
    LegendComponent,
    TooltipComponent,
    CanvasRenderer,
]);

/** Isolated WebGL renderer for the three 3D chart types.  @param {any} p0 @returns {any} */
export default function ECharts3dRenderer({ echartsRef, option }) {
    return (
        <ReactEChartsCore
            ref={echartsRef}
            echarts={echarts}
            option={option}
            style={{ width: '100%', height: '100%' }}
            notMerge={true}
        />
    );
}
