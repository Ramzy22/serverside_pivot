import sys
sys.path.insert(0, '.')

import examples.demo_trader as dt
import examples.demo_hr as dh
import examples.demo_saas as ds
import examples.demo_showcase_hub as dsh

def post_test(app, comp_id, table):
    output = f"{comp_id}.runtimeResponse"
    callback = app.callback_map[output]
    payload = {
        "output": output,
        "outputs": {"id": comp_id, "property": "runtimeResponse"},
        "changedPropIds": [f"{comp_id}.runtimeRequest"],
        "inputs": [
            {
                **item,
                "value": {
                    "schemaVersion": 1,
                    "kind": "data",
                    "requestId": f"req-{comp_id}",
                    "table": table,
                    "payload": {"offset": 0, "limit": 25}
                } if item["property"] == "runtimeRequest" else None
            }
            for item in callback["inputs"]
        ],
        "state": [
            {**item, "value": table if item["property"] == "table" else None}
            for item in callback["state"]
        ],
    }
    client = app.server.test_client()
    resp = client.post("/_dash-update-component", json=payload)
    assert resp.status_code == 200, f"HTTP failure on {comp_id}: {resp.status_code}"
    res_data = resp.json["response"][comp_id]["runtimeResponse"]
    status = res_data.get("status")
    assert status == "data", f"Non-data status on {comp_id}: {status} - {res_data}"
    row_count = len(res_data.get("payload", {}).get("data", []))
    print(f"  [OK] Component: {comp_id:20} -> status: {status} | HTTP 200 | valid data payload received")

print("1. Verifying Trader Demo (Port 8051)...")
post_test(dt.app, "trader-dash-pivot", "trading_portfolio_risk")

print("2. Verifying HR People Analytics Demo (Port 8052)...")
post_test(dh.app, "hr-dash-pivot", "workforce_people_analytics")

print("3. Verifying SaaS Revenue Intelligence Demo (Port 8053)...")
post_test(ds.app, "saas-dash-pivot", "b2b_saas_metrics")

print("4. Verifying Unified Showcase Hub (Trader Tab - Port 8054)...")
post_test(dsh.app, "trader-hub-pivot", "trading_portfolio_risk")

print("5. Verifying Unified Showcase Hub (HR Tab - Port 8054)...")
post_test(dsh.app, "hr-hub-pivot", "workforce_people_analytics")

print("6. Verifying Unified Showcase Hub (SaaS Tab - Port 8054)...")
post_test(dsh.app, "saas-hub-pivot", "b2b_saas_metrics")

print("\n=======================================================")
print("ALL 6 LIVE PIVOT WORKSPACES VERIFIED WITH ZERO ERRORS!")
print("=======================================================")
