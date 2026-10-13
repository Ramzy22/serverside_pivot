import { createGrid, type GridOptions } from 'ag-grid-community';
import { buildReferenceColumnDefs } from './grid-config.ts';
import { registerReferenceModules } from './modules.ts';

import './style.css';

type ReferenceConfig = {
  filterValues: Record<string, unknown[]>;
  options: {
    rowModelType: 'serverSide'; pivotMode: boolean; cellSelection: boolean;
    useValueFormatterForExport: boolean; suppressClipboardPaste: boolean;
    serverSideEnableClientSideSort: boolean; locale: string;
    rowGroupFields: string[]; pivotFields: string[]; valueFields: string[];
    filter: Record<string, string>;
    enableCharts: boolean; rowGroupPanelShow: 'always'; pivotPanelShow: 'always';
    sideBar: { toolPanels: string[] }; cacheBlockSize: number;
    serverSidePivotResultFieldSeparator: string; suppressAggFuncInHeader: boolean;
    defaultColDef: { sortable: boolean; filter: 'agTextColumnFilter'; resizable: boolean; minWidth: number; useValueFormatterForExport: boolean };
    autoGroupColumnDef: { headerName: string; minWidth: number; pinned: 'left' };
    valueFormatter: Intl.NumberFormatOptions & { locale: string };
  };
};

registerReferenceModules();

const response = await fetch('/reference-config.json');
if (!response.ok) throw new Error('Reference configuration unavailable');
const config = await response.json() as ReferenceConfig;
const options = config.options;

const gridOptions: GridOptions = {
  rowModelType: options.rowModelType,
  pivotMode: options.pivotMode,
  cellSelection: options.cellSelection,
  suppressClipboardPaste: options.suppressClipboardPaste,
  serverSideEnableClientSideSort: options.serverSideEnableClientSideSort,
  enableCharts: options.enableCharts,
  rowGroupPanelShow: options.rowGroupPanelShow,
  pivotPanelShow: options.pivotPanelShow,
  sideBar: options.sideBar,
  cacheBlockSize: options.cacheBlockSize,
  serverSidePivotResultFieldSeparator: options.serverSidePivotResultFieldSeparator,
  suppressAggFuncInHeader: options.suppressAggFuncInHeader,
  defaultColDef: options.defaultColDef,
  autoGroupColumnDef: options.autoGroupColumnDef,
  columnDefs: buildReferenceColumnDefs(config),
  initialState: { filter: { filterModel: { scope: { filterType: 'set', values: [options.filter.scope] } } } },
  serverSideDatasource: {
    getRows: async (params) => {
      try {
        const result = await fetch('/api/rows', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(params.request),
        });
        if (!result.ok) throw new Error(`SSRM request failed: ${result.status}`);
        params.success(await result.json());
      } catch (error) {
        console.error(error);
        params.fail();
      }
    },
  },
};

const grid = document.querySelector<HTMLElement>('#grid');
if (!grid) throw new Error('Grid mount is missing');
const api = createGrid(grid, gridOptions);
Object.assign(window, { referenceGrid: api, referenceConfig: config });

document.querySelector('#copy')?.addEventListener('click', () => api.copySelectedRangeToClipboard());
document.querySelector('#csv')?.addEventListener('click', () => api.exportDataAsCsv());
document.querySelector('#excel')?.addEventListener('click', () => api.exportDataAsExcel());
document.querySelector('#save')?.addEventListener('click', () => localStorage.setItem('ag-grid-reference-state', JSON.stringify(api.getState())));
document.querySelector('#restore')?.addEventListener('click', () => {
  const saved = localStorage.getItem('ag-grid-reference-state');
  if (saved) api.setState(JSON.parse(saved));
});
