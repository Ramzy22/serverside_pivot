"""
dash_component.py - A standard Dash AIO component for the Pivot Engine.
Uses dash-ag-grid for rendering and ScalablePivotController for processing.
"""
from typing import List, Dict, Any, Optional, Callable
import uuid
import json
import pandas as pd
from dash import html, dcc, Output, Input, State, callback, MATCH, ALL
import dash_ag_grid as dag

from pivot_engine.grid_hierarchy import build_org_hierarchy_paths
from pivot_engine.runtime.async_bridge import run_awaitable_sync
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import PivotSpec, Measure

class PivotTableAIO(html.Div):
    """
    A Dash All-In-One component that renders a Pivot Table.
    """
    class ids:
        grid = lambda aio_id: {
            'component': 'PivotTableAIO',
            'subcomponent': 'grid',
            'aio_id': aio_id
        }
        store = lambda aio_id: {
            'component': 'PivotTableAIO',
            'subcomponent': 'store',
            'aio_id': aio_id
        }

    ids = ids # Make accessible

    def __init__(
        self,
        aio_id: str = None,
        controller: ScalablePivotController = None,
        table_name: str = None,
        initial_rows: List[str] = [],
        initial_measures: List[Dict[str, Any]] = [],
        initial_filters: List[Dict[str, Any]] = [],
        enable_enterprise_modules: bool = True,  # Fix L6: configurable enterprise modules
        **kwargs
    ):
        """
        Args:
            aio_id: Unique ID for this component instance
            controller: Instance of ScalablePivotController
            table_name: Name of the table to query
            initial_rows: List of dimension columns
            initial_measures: List of measures (dicts with field, agg, alias)
            enable_enterprise_modules: Enable enterprise features (tree data). Default True.
        """
        if aio_id is None:
            aio_id = str(uuid.uuid4())
        self.aio_id = aio_id
        self.controller = controller
        self.table_name = table_name
        
        # Store initial spec in dcc.Store
        initial_spec = {
            "table": table_name,
            "rows": initial_rows,
            "measures": initial_measures,
            "filters": initial_filters,
            "limit": 10000 
        }

        # Build Grid Options
        # We use AG Grid Tree Data mode
        grid = dag.AgGrid(
            id=self.ids.grid(aio_id),
            columnDefs=[], # Will be populated by callback
            rowData=[],    # Will be populated by callback
            defaultColDef={
                "flex": 1,
                "minWidth": 100,
                "sortable": True,
                "resizable": True,
                "filter": True,
            },
            dashGridOptions={
                "getDataPath": {"function": "getDataPath(params)"},
                "treeData": True, 
                "autoGroupColumnDef": {
                    "headerName": "Hierarchy",
                    "minWidth": 250,
                    "cellRendererParams": {
                        "suppressCount": False,
                    },
                },
            },
            enableEnterpriseModules=enable_enterprise_modules,  # Fix L6: use constructor parameter
            # Note: dash-ag-grid includes enterprise bundle, works with watermark without license
            style={"height": "600px", "width": "100%"},
        )

        super().__init__([
            dcc.Store(id=self.ids.store(aio_id), data=initial_spec),
            grid
        ], **kwargs)

    @staticmethod
    def register_callbacks(controller_factory: Callable[[], ScalablePivotController]):
        """
        Register global callbacks.
        controller_factory: Function that returns the controller instance (singleton).
        """
        
        async def _update_grid_async(spec_data):
            if not spec_data:
                return [], []
            
            controller = controller_factory()
            
            # Construct PivotSpec
            measures = [Measure(**m) for m in spec_data['measures']]
            spec = PivotSpec(
                table=spec_data['table'],
                rows=spec_data['rows'],
                measures=measures,
                filters=spec_data.get('filters', []),
                limit=spec_data.get('limit', 1000)
            )

            # Run Hierarchical Pivot (Batch load for depth)
            # This returns a Dict[path_key, list[nodes]]
            # We need to flatten this for AG Grid Tree Data
            
            # We need to construct expanded paths to load *everything* for the initial view
            # OR we just load the root. 
            # Let's load top 2 levels to show it working.
            
            # To load everything "flattened" efficiently, we might want a different query 
            # than the tree manager if we are just dumping to AG Grid.
            # But AG Grid Tree Data expects a "path" column.
            
            # Let's use the standard pivot first to get flat data with grouping cols,
            # then format it for AG Grid.
            # This is simpler than the TreeManager for the initial load.
            
            try:
                # Run standard pivot to get all data (up to limit)
                result = await controller.run_pivot_async(spec, return_format="dict")
            except Exception as e:
                print(f"Error running pivot: {e}")
                return [], []

            # Convert to AG Grid Format
            # AG Grid Tree Data needs:
            # 1. A column for the path: ["North", "Electronics"]
            # 2. Columns for measures.
            
            df = pd.DataFrame(result['rows'], columns=result['columns'])
            
            if df.empty:
                return [], []
            
            row_dims = spec_data['rows']
            
            # Construct 'path' column for AG Grid
            # The path is a list of keys for the hierarchy
            df['orgHierarchy'] = build_org_hierarchy_paths(df, row_dims)
            
            # Column Defs
            # Hide the dimension columns, show only hierarchy + measures
            col_defs = [
                # Hierarchy column is auto-generated by AG Grid, but we need to pass data
                {"field": "orgHierarchy", "hide": True}, 
            ]
            
            for m in measures:
                col_defs.append({
                    "field": m.alias, 
                    "headerName": m.alias.replace('_', ' ').title(),
                    "type": "rightAligned",
                    "valueFormatter": {"function": "d3.format(',.2f')(params.value)"}
                })

            return df.to_dict('records'), col_defs

        @callback(
            Output(PivotTableAIO.ids.grid(MATCH), 'rowData'),
            Output(PivotTableAIO.ids.grid(MATCH), 'columnDefs'),
            Input(PivotTableAIO.ids.store(MATCH), 'data'),
        )
        def update_grid(spec_data):
            return run_awaitable_sync(_update_grid_async(spec_data))
