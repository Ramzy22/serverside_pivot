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
import json
import io
import importlib.util
import re
import shutil
import subprocess
import tarfile
import zipfile
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


def _wheel_verifier_module():
    verifier_path = REPO_ROOT / "scripts" / "ci" / "verify_wheel_contract.py"
    module_spec = importlib.util.spec_from_file_location("wheel_contract_test", verifier_path)
    assert module_spec and module_spec.loader
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def _ci_module(filename, module_name):
    module_spec = importlib.util.spec_from_file_location(
        module_name,
        REPO_ROOT / "scripts" / "ci" / filename,
    )
    assert module_spec and module_spec.loader
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def _write_minimal_wheel(path, members):
    """Create a small wheel-like archive with complete dependency metadata."""
    metadata = "\n".join(
        [
            "Metadata-Version: 2.1",
            "Name: serverside-pivot",
            "Version: 0.0.88",
            "Requires-Dist: dash",
            "Requires-Dist: duckdb",
            "Requires-Dist: ibis-framework",
            "Requires-Dist: pandas",
            "Requires-Dist: pyarrow",
            "",
        ]
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("serverside_pivot-0.0.88.dist-info/METADATA", metadata)
        for member, contents in members.items():
            archive.writestr(member, contents)


def _extract_server_export_order_callback(javascript):
    """Extract the emitted callback that combines live and schema column IDs."""
    callback_start_pattern = re.compile(r"[A-Za-z_$][\w$]*\.useCallback\(\(\) => \{")
    for marker in re.finditer(r"\.total_center_cols\b", javascript):
        prefix_start = max(0, marker.start() - 2_000)
        starts = list(callback_start_pattern.finditer(javascript, prefix_start, marker.start()))
        if not starts:
            continue
        hook_start = starts[-1].start()
        arrow_start = javascript.index("() =>", hook_start, marker.start())
        brace_start = javascript.index("{", arrow_start, marker.start())
        depth = 0
        quote = None
        escaped = False
        for index in range(brace_start, len(javascript)):
            character = javascript[index]
            if quote is not None:
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == quote:
                    quote = None
                continue
            if character in "'\"`":
                quote = character
            elif character == "{":
                depth += 1
            elif character == "}":
                depth -= 1
                if depth == 0:
                    callback = javascript[arrow_start : index + 1]
                    if "total_center_cols" in callback and "__RowTotal__" in callback:
                        return callback
                    break
    raise AssertionError("server export column-order callback missing from wheel JavaScript")


def _run_bundled_server_export_order(javascript):
    """Execute the emitted order callback against a hand-derived reorder fixture."""
    callback = _extract_server_export_order_callback(javascript)
    known_match = re.search(
        r"const\s+([A-Za-z_$][\w$]*)\s*=\s*([A-Za-z_$][\w$]*)\(\);"
        r"\s*if\s*\(!([A-Za-z_$][\w$]*)\)\s*return\s+\1;",
        callback,
    )
    schema_match = re.search(
        r"([A-Za-z_$][\w$]*)\s*&&\s*Number\.isFinite\(Number\(\1\.total_center_cols\)\)",
        callback,
    )
    exportable_match = re.search(
        r"if\s*\(!([A-Za-z_$][\w$]*)\([A-Za-z_$][\w$]*\)\)\s*return\s+null;",
        callback,
    )
    assert known_match and schema_match and exportable_match
    _, known_ids_getter, server_side = known_match.groups()
    schema = schema_match.group(1)
    is_exportable = exportable_match.group(1)
    bindings = {
        known_ids_getter: "() => ['hierarchy', 'center-b', 'center-a', '__RowTotal__sum']",
        server_side: "true",
        schema: (
            "{total_center_cols: 3, columns: "
            "[{id: 'center-a'}, {id: 'center-b'}, {id: 'center-c'}]}"
        ),
        is_exportable: "(columnId) => Boolean(columnId)",
    }
    script = "\n".join(
        [*(f"const {name} = {value};" for name, value in bindings.items()),
         f"const callback = {callback};",
         "process.stdout.write(JSON.stringify(callback()));"]
    )
    result = subprocess.run(
        ["node", "-e", script],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


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
            pattern = parts[1]
            if "*" in pattern or "?" in pattern:
                if not list(PKG_DIR.glob(pattern)):
                    missing.append(str(PKG_DIR / pattern))
            else:
                declared_path = PKG_DIR / pattern
                if not declared_path.exists():
                    missing.append(str(declared_path))

    assert not missing, (
        "MANIFEST.in declares artifact paths that do not exist on disk:\n"
        + "\n".join(f"  {p}" for p in missing)
    )


def test_runtime_schemas_are_versioned_package_dependencies():
    """A clean checkout must contain the runtime validators' schema files."""
    schema_paths = [
        "dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/runtime-contract.schema.json",
        "dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/runtime-response.schema.json",
    ]
    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", *schema_paths],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert set(result.stdout.splitlines()) == set(schema_paths)


def test_wheel_contract_rejects_runtime_schemas_missing_from_distribution(tmp_path):
    wheel_path = tmp_path / "serverside_pivot-0.0.88-py3-none-any.whl"
    _write_minimal_wheel(
        wheel_path,
        {
            "dash_tanstack_pivot/DashTanstackPivot.py": "",
            "dash_tanstack_pivot/dash_tanstack_pivot.min.js": "// dash",
            "dash_tanstack_pivot/package-info.json": "{}",
            "pivot_engine/scalable_pivot_controller.py": "",
            "streamlit_pivot/__init__.py": "",
            "streamlit_pivot/frontend/build/pivot-workspace-manifest.json": "{}",
        },
    )

    violations = _wheel_verifier_module().verify_wheel(wheel_path)

    assert "missing required member: pivot_engine/runtime/runtime-contract.schema.json" in violations
    assert "missing required member: pivot_engine/runtime/runtime-response.schema.json" in violations


def test_wheel_contract_rejects_missing_streamlit_manifest_entry(tmp_path):
    wheel_path = tmp_path / "serverside_pivot-0.0.88-py3-none-any.whl"
    _write_minimal_wheel(
        wheel_path,
        {
            "dash_tanstack_pivot/DashTanstackPivot.py": "",
            "dash_tanstack_pivot/dash_tanstack_pivot.min.js": "// dash",
            "dash_tanstack_pivot/package-info.json": "{}",
            "pivot_engine/scalable_pivot_controller.py": "",
            "pivot_engine/runtime/runtime-contract.schema.json": "{}",
            "pivot_engine/runtime/runtime-response.schema.json": "{}",
            "streamlit_pivot/__init__.py": "",
            "streamlit_pivot/frontend/build/pivot-workspace-manifest.json": (
                '{"src/index.tsx":{"file":"pivot-workspace-missing.js","isEntry":true}}'
            ),
        },
    )

    violations = _wheel_verifier_module().verify_wheel(wheel_path)

    assert "Streamlit manifest entry is missing from wheel: streamlit_pivot/frontend/build/pivot-workspace-missing.js" in violations


def test_clean_wheel_installs_complete_streamlit_component_build(tmp_path):
    """A clean wheel must run Streamlit exclusively from installed assets."""
    source_copy = tmp_path / "package-source"
    shutil.copytree(
        PKG_DIR,
        source_copy,
        ignore=shutil.ignore_patterns(
            ".pytest_cache",
            "node_modules",
            "dist",
            "*.egg-info",
            "__pycache__",
            "pytest-cache-files-*",
        ),
    )
    wheelhouse = tmp_path / "wheelhouse"
    build_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(wheelhouse),
            str(source_copy),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )
    assert build_result.returncode == 0, build_result.stdout + build_result.stderr
    wheels = list(wheelhouse.glob("*.whl"))
    assert len(wheels) == 1
    wheel_path = wheels[0]

    assert _wheel_verifier_module().verify_wheel(wheel_path) == []
    build_prefix = "streamlit_pivot/frontend/build/"
    manifest_name = f"{build_prefix}pivot-workspace-manifest.json"
    with zipfile.ZipFile(wheel_path) as archive:
        names = set(archive.namelist())
        manifest = json.loads(archive.read(manifest_name).decode("utf-8"))
        entry_file = manifest["src/index.tsx"]["file"]
        assert entry_file.startswith("pivot-workspace-") and entry_file.endswith(".js")

        referenced_files = set()
        for record in manifest.values():
            if not isinstance(record, dict):
                continue
            if isinstance(record.get("file"), str):
                referenced_files.add(record["file"])
            for field in ("css", "assets"):
                referenced_files.update(
                    value for value in record.get(field, []) if isinstance(value, str)
                )
            for dependency_key in record.get("imports", []):
                assert dependency_key in manifest
            for dependency_key in record.get("dynamicImports", []):
                assert dependency_key in manifest

        missing_references = sorted(
            relative_path
            for relative_path in referenced_files
            if f"{build_prefix}{relative_path}" not in names
        )
        assert missing_references == []
        assert not any("/node_modules/" in f"/{name}" for name in names)
        assert not any("/.setuptools-build/" in f"/{name}" for name in names)

        wheel_javascript = "\n".join(
            archive.read(name).decode("utf-8")
            for name in sorted(names)
            if name.startswith(build_prefix) and name.endswith(".js")
        )
        assert "pivot-export-csv" in wheel_javascript
        assert "Export format must be either xlsx or csv." in wheel_javascript
        expected_export_order = [
            "hierarchy",
            "center-b",
            "center-a",
            "center-c",
            "__RowTotal__sum",
        ]
        assert _run_bundled_server_export_order(wheel_javascript) == expected_export_order

    install_dir = tmp_path / "installed"
    install_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "--disable-pip-version-check",
            "install",
            "--no-deps",
            "--target",
            str(install_dir),
            str(wheel_path),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )
    assert install_result.returncode == 0, install_result.stdout + install_result.stderr

    component_result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                "import pathlib, sys; "
                f"sys.path.insert(0, {str(install_dir)!r}); "
                "import streamlit_pivot; "
                "package_dir = pathlib.Path(streamlit_pivot.__file__).resolve().parent; "
                "build_dir = package_dir / 'frontend' / 'build'; "
                "entry = streamlit_pivot._component_script(); "
                "assert (build_dir / entry).is_file(); "
                "print(entry)"
            ),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )
    assert component_result.returncode == 0, component_result.stdout + component_result.stderr
    assert component_result.stdout.strip() == entry_file
    installed_build_dir = install_dir / "streamlit_pivot" / "frontend" / "build"
    installed_javascript = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(installed_build_dir.rglob("*.js"))
    )
    assert _run_bundled_server_export_order(installed_javascript) == expected_export_order


def test_host_build_contract_rejects_missing_react_and_streamlit_assets(tmp_path):
    package_dir = tmp_path / "dash_tanstack_pivot"
    streamlit_dir = package_dir / "streamlit_pivot" / "frontend" / "build"
    streamlit_dir.mkdir(parents=True)
    (streamlit_dir / "pivot-workspace-manifest.json").write_text(
        '{"src/index.tsx":{"file":"pivot-workspace-missing.js","isEntry":true}}',
        encoding="utf-8",
    )

    violations = _ci_module(
        "verify_host_build_contract.py", "host_build_contract_test"
    ).verify_host_build(package_dir)

    assert any(violation.endswith("index.mjs") for violation in violations)
    assert any(violation.endswith("react.d.ts") for violation in violations)
    assert "missing Streamlit build asset: pivot-workspace-missing.js" in violations


def test_npm_package_contract_rejects_missing_react_entry(tmp_path):
    archive_path = tmp_path / "dash_tanstack_pivot-0.0.88.tgz"
    with tarfile.open(archive_path, "w:gz") as archive:
        package_json = b'{"name":"dash_tanstack_pivot","exports":{".":{"import":"./dist/react/index.mjs"},"./react":{"import":"./dist/react/index.mjs"}}}'
        info = tarfile.TarInfo("package/package.json")
        info.size = len(package_json)
        archive.addfile(info, fileobj=io.BytesIO(package_json))

    violations = _ci_module(
        "verify_npm_package_contract.py", "npm_package_contract_test"
    ).verify_npm_package(archive_path)

    assert "missing required npm package member: package/dist/react/index.mjs" in violations
    assert "missing required npm package member: package/dist/react/react.d.ts" in violations


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


def test_pyproject_declares_the_documented_pivot_engine_dependencies():
    """A clean install must be able to run the README quick start."""
    data = tomllib.loads((PKG_DIR / "pyproject.toml").read_text(encoding="utf-8"))
    requirements = "\n".join(data["project"].get("dependencies", []))

    for dependency in ("duckdb", "pyarrow", "ibis-framework", "pandas"):
        assert dependency in requirements, (
            f"{dependency} is required by pivot_engine but is missing from pyproject dependencies."
        )


def test_package_metadata_has_a_readme_and_external_build_staging():
    """Repeated local builds must not fold stale staging output into the wheel."""
    data = tomllib.loads((PKG_DIR / "pyproject.toml").read_text(encoding="utf-8"))
    readme = data["project"].get("readme")

    assert readme == "README.md"
    assert (PKG_DIR / readme).is_file()

    setup_cfg = (PKG_DIR / "setup.cfg").read_text(encoding="utf-8")
    assert "build_base = ../.setuptools-build" in setup_cfg


def test_package_lock_matches_package_json_dev_dependencies():
    """CI uses npm ci, so the lockfile must be the declared build graph."""
    package_json = json.loads((PKG_DIR / "package.json").read_text(encoding="utf-8"))
    package_lock = json.loads((PKG_DIR / "package-lock.json").read_text(encoding="utf-8"))
    declared = package_json["devDependencies"]
    locked = package_lock["packages"][""]["devDependencies"]

    assert locked["react-docgen"] == declared["react-docgen"]


def test_combined_host_build_installs_the_streamlit_lockfile():
    """Root build must be reproducible without a pre-existing nested node_modules."""
    package_json = json.loads((PKG_DIR / "package.json").read_text(encoding="utf-8"))
    streamlit_build = package_json["scripts"]["build:streamlit"]

    assert "npm --prefix streamlit_pivot/frontend ci" in streamlit_build
    assert "npm --prefix streamlit_pivot/frontend run build" in streamlit_build


def test_component_asset_metadata_uses_the_distribution_version():
    """A release must not serve a previous version's cached Dash bundle.

    Dash fingerprints component assets with ``dash_tanstack_pivot.__version__``.
    That version comes from the installed package-info.json, rather than from
    pyproject.toml.  Keep every version source aligned so a new Python wheel
    cannot cause browsers to reuse an old JavaScript bundle.
    """
    pyproject_version = tomllib.loads(
        (PKG_DIR / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]["version"]
    metadata_files = [
        PKG_DIR / "package.json",
        PKG_DIR / "package-info.json",
        INNER_PKG_DIR / "package.json",
        INNER_PKG_DIR / "package-info.json",
    ]

    for metadata_file in metadata_files:
        data = json.loads(metadata_file.read_text(encoding="utf-8"))
        assert data["version"] == pyproject_version, (
            f"{metadata_file.relative_to(REPO_ROOT)} declares {data['version']!r}, "
            f"but the distribution declares {pyproject_version!r}."
        )


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


def test_react_package_uses_one_consumer_owned_react_runtime():
    """The standalone package must not bundle a second JSX runtime."""
    package_json = json.loads((PKG_DIR / "package.json").read_text(encoding="utf-8"))
    peer_dependencies = package_json["peerDependencies"]

    assert peer_dependencies["react"] == ">=16.8.6"
    assert peer_dependencies["react-dom"] == ">=16.8.6"
    vite_config = (PKG_DIR / "vite.react.config.mjs").read_text(encoding="utf-8")
    assert "jsxRuntime: 'classic'" in vite_config
    assert "jsx: 'transform'" in vite_config


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
