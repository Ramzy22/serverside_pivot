#!/usr/bin/env bash
# Mutation testing for the semantic core (M1 gate: score >= 80% on
# authority / registry / logical / ir).  mutmut needs fork(), so on Windows
# run this under WSL:  wsl -d Ubuntu -- bash scripts/mutation/run_core_mutation.sh
# Requires a venv with the core test deps + mutmut (default ~/pivot-mut).
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
VENV="${PIVOT_MUT_VENV:-$HOME/pivot-mut}"
WORK="${PIVOT_MUT_WORK:-$HOME/pivot-mutwork}"
rm -rf "$WORK" && mkdir -p "$WORK/tests/security"
cp -r "$REPO/serverside_pivot/pivot_engine/pivot_engine" "$WORK/pivot_engine"
find "$WORK/pivot_engine" -name __pycache__ -prune -exec rm -rf {} +
cp "$REPO/tests/__init__.py" "$REPO/tests/conftest.py" "$WORK/tests/"
cp -r "$REPO/tests/core" "$REPO/tests/oracle" "$REPO/tests/conformance" "$WORK/tests/"
cp "$REPO/tests/security/__init__.py" "$REPO/tests/security/isolation_fixtures.py" \
   "$REPO/tests/security/test_scope_authority.py" "$WORK/tests/security/"
rm -f "$WORK/tests/core/test_architecture.py" "$WORK/tests/conformance/test_legacy_findings.py" \
      "$WORK/tests/oracle/test_engine_invariants.py"
cp "$REPO/scripts/mutation/mutmut.toml" "$WORK/pyproject.toml"
cd "$WORK"
export PIVOT_PROPERTY_SCALE="${PIVOT_PROPERTY_SCALE:-0.2}"
"$VENV/bin/python" -m pytest -q -p no:cacheprovider -x tests >/dev/null  # clean baseline first
"$VENV/bin/mutmut" run "$@" || true
"$VENV/bin/mutmut" results --all true > "$WORK/results.txt" || true
"$VENV/bin/python" "$REPO/scripts/mutation/score.py" "$WORK"
