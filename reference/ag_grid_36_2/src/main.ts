import {
  ColumnApiModule,
  CsvExportModule,
  GridStateModule,
  ModuleRegistry,
  NumberFilterModule,
  TextFilterModule,
  createGrid,
  type GridOptions,
} from 'ag-grid-community';
import {
  CellSelectionModule,
  ClipboardModule,
  ColumnMenuModule,
  ColumnsToolPanelModule,
  ContextMenuModule,
  ExcelExportModule,
  IntegratedChartsModule,
  PivotModule,
  RowGroupingModule,
  RowGroupingPanelModule,
  ServerSideRowModelModule,
  SetFilterModule,
} from 'ag-grid-enterprise';
import { AgChartsEnterpriseModule } from 'ag-charts-enterprise';

import './style.css';

type ReferenceConfig = {
  options: {
    rowModelType: 'serverSide'; pivotMode: boolean; cellSelection: boolean;
    useValueFormatterForExport: boolean; suppressClipboardPaste: boolean;
    serverSideEnableClientSideSort: boolean; locale: string;
    rowGroupFields: string[]; pivotFields: string[]; valueFields: string[];
    filter: Record<string, string>;
    enableCharts: boolean; rowGroupPanelShow: 'always'; pivotPanelShow: 'always';
    sideBar: { toolPanels: string[] }; cacheBlockSize: number;
    serverSidePivotResultFieldSeparator: string; suppressAggFuncInHeader: boolean;
    defaultColDef: { sortable: boolean; filter: boolean; resizable: boolean; minWidth: number; useValueFormatterForExport: boolean };
    autoGroupColumnDef: { headerName: string; minWidth: number; pinned: 'left' };
    valueFormatter: Intl.NumberFormatOptions & { locale: string };
  };
};

ModuleRegistry.registerModules([
  ServerSideRowModelModule, RowGroupingModule, PivotModule, CellSelectionModule,
  ClipboardModule, CsvExportModule, ExcelExportModule,
  IntegratedChartsModule.with(AgChartsEnterpriseModule), GridStateModule,
  ColumnMenuModule, ContextMenuModule, ColumnsToolPanelModule, RowGroupingPanelModule,
  SetFilterModule, TextFilterModule, NumberFilterModule, ColumnApiModule,
]);

const response = await fetch('/reference-config.json');
if (!response.ok) throw new Error('Reference configuration unavailable');
const config = await response.json() as ReferenceConfig;
const options = config.options;
const formatter = new Intl.NumberFormat(options.valueFormatter.locale, options.valueFormatter);
const numeric = (value: unknown): string => typeof value === 'number' ? formatter.format(value) : '';

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
  columnDefs: [
    { field: 'region', rowGroup: true, enableRowGroup: true, filter: 'agSetColumnFilter', hide: true },
    { field: 'account', rowGroup: true, enableRowGroup: true, filter: 'agSetColumnFilter', hide: true },
    { field: 'scope', filter: 'agSetColumnFilter', hide: true },
    { field: 'period', pivot: true, enablePivot: true, filter: 'agSetColumnFilter' },
    ...options.valueFields.map((field) => ({ field, aggFunc: 'sum', enableValue: true, valueFormatter: (params: { value: unknown }) => numeric(params.value) })),
    { field: 'rowId', headerName: 'Row ID' },
    { field: 'extrema', enableValue: true, aggFunc: 'min' },
    { field: 'copyTab' }, { field: 'copyLF' }, { field: 'copyCRLF' },
    { field: 'copyUnicode' }, { field: 'copyNull' }, { field: 'copyEmpty' },
  ],
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
