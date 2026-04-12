from __future__ import annotations

from typing import List

import pandas as pd


def build_org_hierarchy_paths(df: pd.DataFrame, row_dims: List[str]) -> List[List[str]]:
    """Build AG Grid tree-data paths without pandas row-wise apply."""
    if df.empty:
        return []
    available_dims = [dim for dim in (row_dims or []) if dim in df.columns]
    if not available_dims:
        return [[] for _ in range(len(df))]
    hierarchy_values = df.loc[:, available_dims]
    return [
        [str(value) for value in row if pd.notna(value)]
        for row in hierarchy_values.itertuples(index=False, name=None)
    ]
