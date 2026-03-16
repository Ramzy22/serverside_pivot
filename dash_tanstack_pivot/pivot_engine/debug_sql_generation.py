from pivot_engine.planner.sql_planner import SQLPlanner
from pivot_engine.types.pivot_spec import PivotSpec, Measure

print("Creating planner...")
planner = SQLPlanner(dialect='duckdb')

# Create a PivotSpec similar to the failing test
spec = PivotSpec(
    table="sales_pagination",
    rows=["city"],
    measures=[Measure(field="sales", agg="sum", alias="total_sales")],
    sort=[{"field": "city", "order": "asc"}],
    limit=2,
    cursor={"city": "City 1"},
    filters=[]
)

print("Generating plan...")
plan = planner.plan(spec)
print("Plan:", plan)

queries = plan.get("queries", [])
for i, query in enumerate(queries):
    print(f"Query {i}: {query.get('sql')}")
    print(f"Params: {query.get('params')}")
    print()