"""Execute browser tests from the complete committed harness, never checkout helpers."""
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

HARNESS_PATHS = ("tests", "scripts", "examples", "pytest.ini")


def _digest(data):
    return hashlib.sha256(data).hexdigest()


@lru_cache(maxsize=8)
def committed_harness_files(repository: str, commit: str):
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit) is None:
        raise ValueError("invalid harness source commit")
    archive = subprocess.run(["git", "-c", "core.autocrlf=false", "archive", "--format=tar", commit,
                              "--", *HARNESS_PATHS], cwd=repository, check=True, capture_output=True).stdout
    files = {}
    with tarfile.open(fileobj=io.BytesIO(archive)) as contents:
        for member in contents.getmembers():
            name = Path(member.name)
            if name.is_absolute() or ".." in name.parts or member.issym() or member.islnk():
                raise ValueError("unsafe harness archive entry")
            if member.isfile():
                files[member.name] = contents.extractfile(member).read()
    if not all(any(name == path or name.startswith(path + "/") for name in files) for path in HARNESS_PATHS):
        raise ValueError("committed harness closure is incomplete")
    return files


def _check_files(root, files, *, coordinator=False):
    # Python helpers may be imported transitively. Compare the entire Python
    # closure, including unexpected helpers, rather than a hand-picked list.
    expected = {name for name in files if not coordinator or name.endswith(".py") or name == "pytest.ini"}
    actual = {path.relative_to(root).as_posix() for folder in HARNESS_PATHS
              for path in ([root / folder] if (root / folder).is_file() else (root / folder).rglob("*"))
              if path.is_file() and "__pycache__" not in path.parts
              and (not coordinator or path.suffix == ".py" or path.name == "pytest.ini")}
    # Archive runs may generate pytest caches; these are not executable inputs.
    actual = {name for name in actual if "/.pytest_cache/" not in name and not name.endswith(".pyc")}
    if actual != expected:
        raise ValueError("executing harness file closure differs from committed source")
    for name in expected:
        current = (root / name).read_bytes()
        committed = files[name]
        if coordinator:
            current, committed = current.replace(b"\r\n", b"\n"), committed.replace(b"\r\n", b"\n")
        if current != committed:
            raise ValueError(f"executing harness helper differs from committed source: {name}")


@dataclass(frozen=True)
class BrowserHarness:
    root: Path
    record: Path


@contextmanager
def committed_browser_harness(binding, evidence_root, *, coordinator_root=None):
    evidence_root = Path(evidence_root).resolve()
    if evidence_root.name != binding.run_id or evidence_root.parent.name != "runs":
        raise ValueError("harness evidence must belong to current run")
    repository = Path(binding.paths["source_repository"]).resolve()
    coordinator = Path(coordinator_root or Path(__file__).resolve().parents[1]).resolve()
    files = committed_harness_files(str(repository), binding.source_commit)
    _check_files(coordinator, files, coordinator=True)
    record = evidence_root / "browser-harness.json"
    # Remove a previous completion before attempting another run.
    record.unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix="pf-browser-harness-") as directory:
        source = Path(directory)
        for name, content in files.items():
            target = source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        _check_files(source, files)
        try:
            yield BrowserHarness(source, record)
        finally:
            _check_files(source, files)
            _check_files(coordinator, files, coordinator=True)
        record.write_text(json.dumps({"runId": binding.run_id, "sourceCommit": binding.source_commit,
            "artifactSha256": dict(binding.sha256), "completed": True,
            "execution": "committed archive; coordinator and archive verified before and after execution",
            "committedHarnessSha256": {name: _digest(data) for name, data in sorted(files.items())}},
            sort_keys=True, indent=2) + "\n", encoding="utf-8")


def validate_harness_record(binding, path):
    record = json.loads(Path(path).read_text(encoding="utf-8"))
    files = committed_harness_files(str(Path(binding.paths["source_repository"]).resolve()), binding.source_commit)
    if (record.get("runId") != binding.run_id or record.get("sourceCommit") != binding.source_commit
            or record.get("artifactSha256") != binding.sha256 or record.get("completed") is not True
            or record.get("committedHarnessSha256") != {name: _digest(data) for name, data in sorted(files.items())}):
        raise ValueError("harness record differs from executed committed closure")
    return record
