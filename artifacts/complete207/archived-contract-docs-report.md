# Archived contract documentation repair

Base: `064c4e95ddde2cb6cca204facf3b0ed331000536`.

The documentation contract still read root-level files after the archival cleanup. The architecture document already exists at `archive/docs/CAPABILITY_ARCHITECTURE.md`; the cleanup ledger was absent. The test now reads both documents in `archive/docs`. The ledger is restored there from `df63d5f:REFACTOR_CLEANUP_LEDGER.md`, with its terminal empty line removed; all historical prose and table content is preserved. All three required content assertions remain unchanged. No root-level documentation or product behavior was added.

## Red

Interpreter: `C:/Users/ramzy/AppData/Local/Temp/pf-sec207/Scripts/python.exe`.

Command: `python -m pytest tests/test_runtime_capability_architecture.py::test_capability_architecture_and_cleanup_decisions_are_recorded -q`

Result before edits: exit 1; `1 failed, 1 warning in 0.26s`. Failure: `FileNotFoundError` reading root `CAPABILITY_ARCHITECTURE.md`, at test line 74.

## Green

Command with the same interpreter: `python -m pytest tests/test_runtime_capability_architecture.py -q`

Result after edits: exit 0; `4 passed, 1 warning in 0.33s`. Existing pytest-asyncio configuration and Dash plugin deprecation warnings appeared. Before EOF normalization, a separate byte equality check confirmed the restored ledger exactly matched the historical Git blob. No browser or full-suite tests were run.

## Additional lifecycle document path

Expanded scope only to the documentation path in `test_runtime_request_lifecycle_spec_documents_profile_contract`, per parent assignment. Its reference now reads `docs/RUNTIME_REQUEST_LIFECYCLE.md`; its 13 required contract strings are unchanged. No runtime code changed.

Before this edit, running `python -m pytest tests/test_runtime_request_coordinator.py::test_runtime_request_lifecycle_spec_documents_profile_contract -q` with the same secure interpreter exited 1: `1 failed, 1 warning in 3.89s`, with `FileNotFoundError` for root `RUNTIME_REQUEST_LIFECYCLE.md` at line 35.

Final focused command: `python -m pytest tests/test_runtime_capability_architecture.py tests/test_runtime_request_coordinator.py::test_runtime_request_lifecycle_spec_documents_profile_contract -q`.

Final result: exit 0; `5 passed, 1 warning in 2.85s`. Only the existing dependency deprecation/configuration warnings appeared. `git diff --check` passed for the tracked test edits. The initial staged ledger inherited a terminal blank line at line 21, which `git diff --cached --check` flagged. A follow-up removed only that terminal empty line, and the final aggregate whitespace check passes. Parent full-suite evidence reference: `artifacts/complete207/final-064c4e9-python.log`, lines 144-203; no full suite was run for this change.

## Historical provenance and EOF normalization

Historical source: `df63d5f:REFACTOR_CLEANUP_LEDGER.md`, Git blob `c47776376da977e8c97fcbc06e276abab1679de7`. The initial restored blob in commit `e26ed8b373cf1d107bb7b652ef53cebe6ca565d4` was also `c47776376da977e8c97fcbc06e276abab1679de7`. The final file preserves historical content with EOF whitespace normalization, so it is not blob-identical to that source. Only the terminal empty line was removed; no headings, prose, table entries, or assertions changed.
