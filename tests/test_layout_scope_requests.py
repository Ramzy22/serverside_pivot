"""A layout revision is only valid for the structural query that created it."""

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


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
