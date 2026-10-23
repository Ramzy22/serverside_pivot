import { createGrid, ModuleRegistry, RowApiModule, type GridOptions } from 'ag-grid-community';
import { ServerSideRowModelApiModule } from 'ag-grid-enterprise';
import { buildReferenceColumnDefs } from './grid-config.ts';
import { registerReferenceModules } from './modules.ts';

import './style.css';

type ReferenceConfig = {
  modules: string[];
  filterValues: Record<string, unknown[]>;
  options: {
    rowModelType: 'serverSide'; pivotMode: boolean; cellSelection: boolean;
    grandTotalRow: 'bottom'; pivotRowTotals: 'after';
    useValueFormatterForExport: boolean; suppressClipboardPaste: boolean;
    serverSideEnableClientSideSort: boolean; locale: string;
    rowGroupFields: string[]; pivotFields: string[]; valueFields: string[];
    filter: Record<string, string>;
    enableCharts: boolean; rowGroupPanelShow: 'always'; pivotPanelShow: 'always';
    sideBar: { toolPanels: string[] }; cacheBlockSize: number;
    serverSidePivotResultFieldSeparator: string; suppressAggFuncInHeader: boolean;
    defaultColDef: { sortable: boolean; filter: 'agTextColumnFilter'; resizable: boolean; minWidth: number; useValueFormatterForExport: boolean };
    autoGroupColumnDef: { headerName: string; minWidth: number; pinned: 'left'; filter: 'agGroupColumnFilter' };
    valueFormatter: Intl.NumberFormatOptions & { locale: string };
  };
};

registerReferenceModules();
// Required only by the read-only displayed-row and expansion evidence probes.
ModuleRegistry.registerModules([RowApiModule, ServerSideRowModelApiModule]);

// Read-only browser evidence for the local parity harness. The request/response
// copies identify which visible state came from the real SSRM datasource.
const datasourceTrace: Array<{ request: unknown; response?: unknown; error?: string }> = [];

const scenario = new URLSearchParams(window.location.search).get('scenario') === 'scroll' ? '?scenario=scroll' : '';
const response = await fetch(`/reference-config.json${scenario}`);
if (!response.ok) throw new Error('Reference configuration unavailable');
const config = await response.json() as ReferenceConfig;
const options = config.options;
let globalSearch = '';

const gridOptions: GridOptions = {
  rowModelType: options.rowModelType,
  pivotMode: options.pivotMode,
  grandTotalRow: options.grandTotalRow,
  pivotRowTotals: options.pivotRowTotals,
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
  columnDefs: buildReferenceColumnDefs(config, '', scenario),
  initialState: { filter: { filterModel: { scope: { filterType: 'set', values: [options.filter.scope] } } } },
  serverSideDatasource: {
    getRows: async (params) => {
      const request = { ...structuredClone(params.request), globalSearch };
      const entry: { request: unknown; response?: unknown; error?: string } = {
        request,
      };
      datasourceTrace.push(entry);
      try {
        const result = await fetch(`/api/rows${scenario}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(request),
        });
        if (!result.ok) throw new Error(`SSRM request failed: ${result.status}`);
        const body = await result.json();
        entry.response = structuredClone(body);
        params.success(body);
      } catch (error) {
        entry.error = String(error);
        console.error(error);
        params.fail();
      }
    },
  },
};

const grid = document.querySelector<HTMLElement>('#grid');
if (!grid) throw new Error('Grid mount is missing');
const api = createGrid(grid, gridOptions);
const searchForm = document.querySelector<HTMLFormElement>('#global-search-form');
const searchInput = document.querySelector<HTMLInputElement>('#global-search');
if (!searchForm || !searchInput) throw new Error('Reference search controls are missing');
searchForm.addEventListener('submit', (event) => {
  event.preventDefault();
  globalSearch = searchInput.value.trim();
  api.refreshServerSide({ purge: true });
});
document.querySelector('#search-clear')?.addEventListener('click', () => {
  searchInput.value = '';
  globalSearch = '';
  api.refreshServerSide({ purge: true });
});
Object.assign(window, {
  referenceGrid: api,
  referenceConfig: {
    ...config,
    modules: [...new Set([...config.modules, 'RowApiModule', 'ServerSideRowModelApiModule'])],
  },
});
const referenceEvidence = {
  get ready() { return grid.querySelector('[role="gridcell"]') !== null && datasourceTrace.some((entry) => entry.response); },
  trace: () => structuredClone(datasourceTrace),
  columnModel: () => ({
    filterModel: structuredClone(api.getState().filter?.filterModel ?? {}),
    values: api.getValueColumns().map((column) => ({
      field: column.getColDef().field,
      aggFunc: column.getAggFunc(),
    })),
    pivots: api.getPivotColumns().map((column) => column.getColId()),
  }),
  displayedRows: () => Array.from({ length: api.getDisplayedRowCount() }, (_, index) => {
    const node = api.getDisplayedRowAtIndex(index);
    return {
      index,
      id: node?.id ?? null,
      label: node?.footer ? 'Total' : node?.key ?? '',
      parentKey: node?.parent?.key ?? null,
      level: node?.level ?? null,
      footer: node?.footer === true,
      expanded: node?.expanded === true,
      data: structuredClone(node?.data ?? null),
    };
  }),
  visibleCells: () => Array.from(grid.querySelectorAll<HTMLElement>('[role="gridcell"]'))
    .filter((cell) => cell.getClientRects().length > 0)
    .map((cell) => ({
      rowIndex: cell.closest<HTMLElement>('[row-index]')?.getAttribute('row-index') ?? '',
      colId: cell.getAttribute('col-id') ?? '',
      text: cell.innerText,
    })),
  headerGeometry: () => Array.from(grid.querySelectorAll<HTMLElement>('.ag-header-cell[col-id]'))
    .filter((header) => header.getClientRects().length > 0)
    .map((header) => ({
      colId: header.getAttribute('col-id') ?? '',
      width: header.getBoundingClientRect().width,
      left: header.getBoundingClientRect().left,
    })),
};
Object.assign(window, { referenceEvidence });

document.querySelector('#copy')?.addEventListener('click', () => api.copySelectedRangeToClipboard());
document.querySelector('#csv')?.addEventListener('click', () => api.exportDataAsCsv());
document.querySelector('#excel')?.addEventListener('click', () => api.exportDataAsExcel());
document.querySelector('#save')?.addEventListener('click', () => localStorage.setItem('ag-grid-reference-state', JSON.stringify(api.getState())));
document.querySelector('#restore')?.addEventListener('click', () => {
  const saved = localStorage.getItem('ag-grid-reference-state');
  if (saved) api.setState(JSON.parse(saved));
});
