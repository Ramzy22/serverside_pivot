import type { ColDef, SetFilterValuesFuncParams } from 'ag-grid-community';

export type ReferenceConfig = {
  filterValues: Record<string, unknown[]>;
  options: {
    locale: string;
    valueFields: string[];
    valueFormatter: Intl.NumberFormatOptions & { locale: string };
  };
};

const aggregateChoices = ['sum', 'avg', 'min', 'max', 'count', 'first', 'last'];

export function buildReferenceColumnDefs(config: ReferenceConfig, origin = ''): ColDef[] {
  const { options } = config;
  const formatter = new Intl.NumberFormat(options.valueFormatter.locale, options.valueFormatter);
  const numeric = (value: unknown): string => typeof value === 'number' ? formatter.format(value) : '';
  const setFilter = (field: string, extra: ColDef = {}): ColDef => ({
    field,
    filter: 'agSetColumnFilter',
    filterParams: {
      values: (params: SetFilterValuesFuncParams) => {
        void fetch(`${origin}/api/filter-values`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ field, filterModel: params.api.getFilterModel() }),
        }).then(async (response) => {
          if (!response.ok) throw new Error(`Set Filter values request failed: ${response.status}`);
          return response.json();
        }).then((result) => params.success(result.values)).catch((error) => {
          console.error(error);
          params.success([]);
        });
      },
      refreshValuesOnOpen: true,
      suppressClearModelOnRefreshValues: true,
    },
    ...extra,
  });
  return [
    setFilter('region', { rowGroup: true, enableRowGroup: true, hide: true }),
    setFilter('account', { rowGroup: true, enableRowGroup: true, hide: true }),
    setFilter('scope', { hide: true }),
    setFilter('period', { pivot: true, enablePivot: true }),
    ...options.valueFields.map((field): ColDef => ({
      field,
      aggFunc: 'sum',
      allowedAggFuncs: aggregateChoices,
      enableValue: true,
      filter: 'agNumberColumnFilter',
      valueFormatter: (params) => numeric(params.value),
    })),
    { field: 'rowId', headerName: 'Row ID' },
    { field: 'extrema', enableValue: true, aggFunc: 'min', allowedAggFuncs: aggregateChoices, filter: 'agNumberColumnFilter' },
    { field: 'copyTab' }, { field: 'copyLF' }, { field: 'copyCRLF' },
    { field: 'copyUnicode' }, { field: 'copyNull' }, { field: 'copyEmpty' },
  ];
}
