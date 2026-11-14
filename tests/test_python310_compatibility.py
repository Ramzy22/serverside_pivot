"""Exercise supported-Python imports without loading optional engine backends."""

from contextlib import contextmanager
from datetime import datetime, timezone
import builtins
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "serverside_pivot" / "pivot_engine" / "pivot_engine"


@contextmanager
def load_governor(*, force_fallback=False):
    # A private package bypasses __init__ and its optional data backends while
    # retaining real relative imports and psutil in the module under test.
    package_name = "_python310_compat_engine"
    package = ModuleType(package_name)
    package.__path__ = [str(ENGINE)]
    original_import = builtins.__import__

    def import_without_strenum(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "enum" and "StrEnum" in fromlist:
            raise ImportError("StrEnum is unavailable on Python 3.10")
        return original_import(name, globals, locals, fromlist, level)

    try:
        sys.modules[package_name] = package
        with patch("builtins.__import__", import_without_strenum if force_fallback else original_import):
            yield importlib.import_module(f"{package_name}.resource_governor")
    finally:
        for name in list(sys.modules):
            if name == package_name or name.startswith(package_name + "."):
                del sys.modules[name]


class Python310CompatibilityTests(unittest.TestCase):
    def test_resource_kind_retains_string_contract_on_supported_pythons(self):
        for force_fallback in (False, True):
            with self.subTest(force_fallback=force_fallback), load_governor(force_fallback=force_fallback) as governor:
                kinds = governor.ResourceKind
                for value in ("cache", "payload", "query", "export", "download", "cdc"):
                    with self.subTest(value=value):
                        kind = kinds(value)
                        self.assertIsInstance(kind, str)
                        self.assertEqual(kind, value)
                        self.assertEqual(hash(kind), hash(value))
                        self.assertEqual(str(kind), value)
                        self.assertEqual(f"{kind}", value)
                        self.assertEqual(format(kind, ">10"), value.rjust(10))
                        self.assertEqual(json.dumps({"kind": kind}), '{"kind": "' + value + '"}')
                        self.assertIs(kinds[value.upper()], kind)
                        self.assertIs(kinds(kind), kind)
                with self.assertRaises(ValueError):
                    kinds("unknown")

    def test_certification_script_imports_with_utc_timestamps(self):
        path = ROOT / "scripts" / "certify_flagship.py"
        spec = importlib.util.spec_from_file_location("_python310_compat_certify", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertIs(module.UTC, timezone.utc)
        self.assertEqual(datetime(2026, 1, 1, tzinfo=module.UTC).isoformat(), "2026-01-01T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
