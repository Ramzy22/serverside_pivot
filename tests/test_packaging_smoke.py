"""
Packaging smoke tests for dash-tanstack-pivot.

These tests verify:
- Bundle artifact contract: the expected minified JS bundle is present after npm build
- Package metadata contract: pyproject.toml declares the correct package name and redis extra
- Import smoke: the installed package is importable with no missing dependencies

Run via:
    python -m pytest tests/test_packaging_smoke.py -q

Full deterministic release gate (install + import + multi-instance regression):
    python -m build dash_tanstack_pivot
    python -m pip install --force-reinstall ./dash_tanstack_pivot
    python -m pip install --force-reinstall "./dash_tanstack_pivot[redis]"
    python -c "import dash_tanstack_pivot; print('import-ok')"
    python -m pytest tests/test_packaging_smoke.py tests/test_dash_runtime_callbacks.py tests/test_session_request_gate.py -q
"""

import pathlib
import sys
try:
    import tomllib
except ImportError:
    import tomli as tomllib


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ROOT = pathlib.Path(__file__).parent.parent
PKG_DIR = REPO_ROOT / "dash_tanstack_pivot"
INNER_PKG_DIR = PKG_DIR / "dash_tanstack_pivot"


# ---------------------------------------------------------------------------
# Bundle artifact contract
# ---------------------------------------------------------------------------


def test_bundle_artifact_contract():
    """The minified JS bundle must exist at the path declared by package metadata."""
    bundle = INNER_PKG_DIR / "dash_tanstack_pivot.min.js"
    assert bundle.exists(), (
        f"Expected minified bundle not found at {bundle}. "
        "Run 'npm run build' inside dash_tanstack_pivot/ to produce it."
    )
    assert bundle.stat().st_size > 0, f"Bundle file {bundle} is empty."


def test_manifest_declares_only_existing_artifacts():
    """MANIFEST.in must not reference files that do not exist in the repo."""
    manifest = PKG_DIR / "MANIFEST.in"
    assert manifest.exists(), "MANIFEST.in is missing from dash_tanstack_pivot/"

    missing = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Lines are like: include path/to/file  or  recursive-include dir pattern
        parts = line.split()
        if parts[0] == "include" and len(parts) >= 2:
            declared_path = PKG_DIR / parts[1]
            if not declared_path.exists():
                missing.append(str(declared_path))

    assert not missing, (
        "MANIFEST.in declares artifact paths that do not exist on disk:\n"
        + "\n".join(f"  {p}" for p in missing)
    )


# ---------------------------------------------------------------------------
# Package metadata contract
# ---------------------------------------------------------------------------


def test_pyproject_toml_name():
    """pyproject.toml must declare name = 'serverside-pivot'."""
    pyproject = PKG_DIR / "pyproject.toml"
    assert pyproject.exists(), "pyproject.toml is missing from dash_tanstack_pivot/"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    assert data["project"]["name"] == "serverside-pivot", (
        f"Expected project name 'serverside-pivot', got '{data['project']['name']}'"
    )


def test_pyproject_toml_redis_extra():
    """pyproject.toml must declare optional-dependencies.redis."""
    pyproject = PKG_DIR / "pyproject.toml"
    assert pyproject.exists(), "pyproject.toml is missing from dash_tanstack_pivot/"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    optional = data["project"].get("optional-dependencies", {})
    assert "redis" in optional, (
        "optional-dependencies.redis is not declared in pyproject.toml. "
        "Consumers need 'pip install serverside-pivot[redis]' to be valid."
    )
    assert optional["redis"], "optional-dependencies.redis list is empty."


def test_package_json_no_broken_prepublish():
    """package.json must not reference a prepublishOnly hook that calls a missing script."""
    import json

    pkg_json = PKG_DIR / "package.json"
    assert pkg_json.exists(), "package.json missing from dash_tanstack_pivot/"
    data = json.loads(pkg_json.read_text(encoding="utf-8"))
    scripts = data.get("scripts", {})

    # validate-init script referenced a missing _validate_init.py and must not exist
    assert "validate-init" not in scripts, (
        "package.json still declares validate-init which references a missing _validate_init.py"
    )
    assert "prepublishOnly" not in scripts, (
        "package.json still has prepublishOnly hook referencing missing validate-init script"
    )


def test_package_json_uses_strict_docgen_build():
    """build:py must fail when Dash/react-docgen emits parser errors."""
    import json

    pkg_json = PKG_DIR / "package.json"
    data = json.loads(pkg_json.read_text(encoding="utf-8"))
    scripts = data.get("scripts", {})
    strict_script = PKG_DIR / "scripts" / "build_py_strict.py"

    assert scripts.get("build:py") == "python scripts/build_py_strict.py"
    assert strict_script.exists(), "Strict Dash component generator wrapper is missing."

    source = strict_script.read_text(encoding="utf-8")
    assert "dash.development.component_generator" in source
    assert "Error with path" in source
    assert "did not recognize object of type" in source
    assert "return 1" in source


# ---------------------------------------------------------------------------
# Import smoke
# ---------------------------------------------------------------------------


def test_import_dash_tanstack_pivot():
    """Importing dash_tanstack_pivot must not raise ImportError or missing-dep errors."""
    # Ensure the local source is importable even without a dist install
    if str(PKG_DIR) not in sys.path:
        sys.path.insert(0, str(PKG_DIR))

    import importlib

    mod = importlib.import_module("dash_tanstack_pivot")
    assert hasattr(mod, "DashTanstackPivot"), (
        "dash_tanstack_pivot module does not expose DashTanstackPivot component"
    )


def test_generated_component_exposes_runtime_transport_props():
    """The generated Python wrapper must expose the unified runtime transport props."""
    if str(PKG_DIR) not in sys.path:
        sys.path.insert(0, str(PKG_DIR))

    import importlib

    mod = importlib.import_module("dash_tanstack_pivot")
    component = mod.DashTanstackPivot(id="pivot-grid")

    assert "runtimeRequest" in component.available_properties
    assert "runtimeResponse" in component.available_properties
    assert "viewMode" in component.available_properties
    assert "detailMode" in component.available_properties
    assert "treeConfig" in component.available_properties
    assert "detailConfig" in component.available_properties
    assert "performanceConfig" in component.available_properties
    assert "pivotMode" not in component.available_properties
    for legacy_prop in (
        "filterRequest",
        "filterOptions",
        "chartRequest",
        "chartData",
        "drillThrough",
        "viewport",
        "rowCount",
        "dataOffset",
        "dataVersion",
        "columns",
    ):
        assert legacy_prop not in component.available_properties
