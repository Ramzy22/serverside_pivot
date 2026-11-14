from pathlib import Path


MAX_AUTHORED_SOURCE_LINES = 5_000
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AUTHORED_SOURCE_ROOTS = (
    REPOSITORY_ROOT / "serverside_pivot" / "src" / "lib",
    REPOSITORY_ROOT / "serverside_pivot" / "pivot_engine",
)


def _authored_source_files():
    for root in AUTHORED_SOURCE_ROOTS:
        yield from root.rglob("*.js")
        yield from root.rglob("*.py")


def test_authored_source_modules_remain_within_the_line_limit():
    oversized = []
    for source_file in _authored_source_files():
        if "__pycache__" in source_file.parts or "node_modules" in source_file.parts:
            continue
        line_count = len(source_file.read_text(encoding="utf-8").splitlines())
        if line_count > MAX_AUTHORED_SOURCE_LINES:
            oversized.append((source_file.relative_to(REPOSITORY_ROOT), line_count))

    assert not oversized, (
        f"Authored source files must stay at or below {MAX_AUTHORED_SOURCE_LINES:,} lines: "
        + ", ".join(f"{path} ({line_count:,})" for path, line_count in oversized)
    )
