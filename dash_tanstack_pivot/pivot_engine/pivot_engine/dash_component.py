"""
dash_component.py - A standard Dash AIO component for the Pivot Engine.
Uses dash-ag-grid for rendering and ScalablePivotController for processing.
"""
from typing import List, Dict, Any, Optional
import uuid
import json
import pandas as pd
from dash import html, dcc, Output, Input, State, callback, MATCH, ALL
import dash_ag_grid as dag

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
        **kwargs
    ):
        """
        Args:
            aio_id: Unique ID for this component instance
            controller: Instance of ScalablePivotController
            table_name: Name of the table to query
            initial_rows: List of dimension columns
            initial_measures: List of measures (dicts with field, agg, alias)
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
            enableEnterpriseModules=True, # Tree data needs enterprise (or valid polyfill logic)
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
        
        @callback(
            Output(PivotTableAIO.ids.grid(MATCH), 'rowData'),
            Output(PivotTableAIO.ids.grid(MATCH), 'columnDefs'),
            Input(PivotTableAIO.ids.store(MATCH), 'data'),
        )
        def update_grid(spec_data):
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
            
            # For simplicity in V1, we fetch the first N levels
            # A more advanced version would use expanded_paths from grid state
            expanded_paths = [] # Default to collapsed
            
            # Note: run_hierarchical_pivot_batch_load is async. 
            # Dash callbacks support async if using Quart or standard Flask with sync wrapper.
            # Since controller is async, we need to run it synchronously here or make callback async.
            # Dash supports async callbacks natively now.
            
            import asyncio
            try:
                # Get the running loop or create a new one
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # We want depth=len(rows) to see full tree, or limit it
            depth = len(spec_data['rows'])
            
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
                result = loop.run_until_complete(controller.run_pivot_async(spec, return_format="dict"))
            except Exception as e:
                # If loop is already running (e.g. uvicorn), we can't run_until_complete.
                # In standard Dash (Flask), this works.
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
            def make_path(row):
                path = []
                for dim in row_dims:
                    val = row.get(dim)
                    if val is not None:
                        path.append(str(val))
                return path

            df['orgHierarchy'] = df.apply(make_path, axis=1)
            
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

