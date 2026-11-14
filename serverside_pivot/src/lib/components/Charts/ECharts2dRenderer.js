import React from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { BarChart, BoxplotChart } from 'echarts/charts';
import {
    DatasetComponent,
    GridComponent,
    LegendComponent,
    PolarComponent,
    TooltipComponent,
    TransformComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

// Register only the ECharts pieces used by range, box plot, and nightingale.
// The default echarts-for-react entry imports every chart type, which is not
// appropriate for this opt-in renderer.
echarts.use([
    BarChart,
    BoxplotChart,
    DatasetComponent,
    GridComponent,
    LegendComponent,
    PolarComponent,
    TooltipComponent,
    TransformComponent,
    CanvasRenderer,
]);

/** @param {any} p0 @returns {any} */
export default function ECharts2dRenderer({ echartsRef, option }) {
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
