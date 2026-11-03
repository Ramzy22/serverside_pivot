import {
  ColumnApiModule, ColumnAutoSizeModule, CsvExportModule, GridStateModule, ModuleRegistry,
  NumberFilterModule, TextFilterModule, CellStyleModule, ScrollApiModule,
} from 'ag-grid-community';
import {
  CellSelectionModule, ClipboardModule, ColumnMenuModule, ColumnsToolPanelModule,
  ContextMenuModule, ExcelExportModule, FiltersToolPanelModule, IntegratedChartsModule,
  PivotModule, RowGroupingModule, RowGroupingPanelModule, ServerSideRowModelModule,
  SetFilterModule, GroupFilterModule,
} from 'ag-grid-enterprise';
import { AgChartsEnterpriseModule } from 'ag-charts-enterprise';

const referenceModules = [
  ServerSideRowModelModule, RowGroupingModule, PivotModule, CellSelectionModule,
  ClipboardModule, CsvExportModule, ExcelExportModule,
  IntegratedChartsModule.with(AgChartsEnterpriseModule), GridStateModule,
  ColumnMenuModule, ContextMenuModule, ColumnsToolPanelModule, FiltersToolPanelModule,
  RowGroupingPanelModule, SetFilterModule, GroupFilterModule, TextFilterModule, NumberFilterModule,
  ColumnApiModule, ColumnAutoSizeModule, CellStyleModule, ScrollApiModule,
];

export function registerReferenceModules() {
  ModuleRegistry.registerModules(referenceModules);
  return referenceModules;
}
