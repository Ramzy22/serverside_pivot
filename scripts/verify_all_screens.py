import sys
sys.path.insert(0, '.')

import examples.demo_trader as dt
import examples.demo_hr as dh
import examples.demo_saas as ds
import examples.demo_showcase_hub as dsh
from examples.demo_datasets import (
    trader_workspace_config,
    hr_workspace_config,
    saas_workspace_config,
)

def post_test(app, comp_id, cfg):
    table = cfg["table"]
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
                    "stateEpoch": 1,
                    "windowSeq": 1,
                    "abortGeneration": 1,
                    "payload": {"offset": 0, "limit": 30},
                } if item["property"] == "runtimeRequest" else cfg.get(item["property"])
            }
            for item in callback["inputs"]
        ],
        "state": [
            {**item, "value": cfg.get(item["property"])}
            for item in callback["state"]
        ],
    }
    client = app.server.test_client()
    resp = client.post("/_dash-update-component", json=payload)
    assert resp.status_code == 200, f"HTTP failure on {comp_id}: {resp.status_code}"
    res_data = resp.json["response"][comp_id]["runtimeResponse"]
    status = res_data.get("status")
    assert status == "data", f"Non-data status on {comp_id}: {status} - {res_data}"
    data_rows = res_data.get("payload", {}).get("data", [])
    real_rows = [r for r in data_rows if r.get("_path") != "__color_scale_stats__"]
    assert len(real_rows) > 0, f"Zero data rows returned on {comp_id}! Full payload: {res_data}"
    print(f"  [OK] Component: {comp_id:20} -> status: {status} | HTTP 200 | {len(real_rows)} rendered hierarchy rows")

print("1. Verifying Trader Demo (Port 8051)...")
post_test(dt.app, "trader-dash-pivot", trader_workspace_config("dash"))

print("2. Verifying HR People Analytics Demo (Port 8052)...")
post_test(dh.app, "hr-dash-pivot", hr_workspace_config("dash"))

print("3. Verifying SaaS Revenue Intelligence Demo (Port 8053)...")
post_test(ds.app, "saas-dash-pivot", saas_workspace_config("dash"))

print("4. Verifying Unified Showcase Hub (Trader Tab - Port 8054)...")
post_test(dsh.app, "trader-hub-pivot", trader_workspace_config("hub"))

print("5. Verifying Unified Showcase Hub (HR Tab - Port 8054)...")
post_test(dsh.app, "hr-hub-pivot", hr_workspace_config("hub"))

print("6. Verifying Unified Showcase Hub (SaaS Tab - Port 8054)...")
post_test(dsh.app, "saas-hub-pivot", saas_workspace_config("hub"))

print("\n=======================================================")
print("ALL 6 LIVE PIVOT WORKSPACES VERIFIED WITH REAL DATA ROWS!")
print("=======================================================")
