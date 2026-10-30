"""A layout revision is only valid for the structural query that created it."""

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_frontend_layout_commit_rejects_mixed_and_superseded_deliveries():
    completed = subprocess.run(
        ["node", str(Path(__file__).with_name("frontend_pivot_layout_contract.cjs"))],
        cwd=ROOT, text=True, capture_output=True, timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "Pivot layout client contract passed" in completed.stdout


def test_filter_scope_change_resends_manifest_without_reusing_revision():
    script = """
      import { pivotLayoutRequestFields } from './dash_tanstack_pivot/src/lib/utils/columnOrder.js';
      const args = [['path:q1|sales'], 'old-revision', 'path:q1|sales', [], null, false, {}, true];
      console.log(JSON.stringify(pivotLayoutRequestFields(...args)));
    """
    completed = subprocess.run(
        ["node", "--experimental-default-type=module", "--input-type=module", "-e", script],
        cwd=ROOT, check=True, text=True, capture_output=True,
    )
    fields = json.loads(completed.stdout)
    assert fields["layoutManifest"] == ["path:q1|sales"]
    assert "layoutRevision" not in fields


def test_unchanged_scope_uses_compact_revision():
    script = """
      import { pivotLayoutRequestFields } from './dash_tanstack_pivot/src/lib/utils/columnOrder.js';
      const args = [['path:q1|sales'], 'known-revision', 'path:q1|sales', [], null, false, {}, false];
      console.log(JSON.stringify(pivotLayoutRequestFields(...args)));
    """
    completed = subprocess.run(
        ["node", "--experimental-default-type=module", "--input-type=module", "-e", script],
        cwd=ROOT, check=True, text=True, capture_output=True,
    )
    fields = json.loads(completed.stdout)
    assert fields["layoutRevision"] == "known-revision"
    assert "layoutManifest" not in fields
