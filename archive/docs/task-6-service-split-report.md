# Task 6 export capability split

The existing architecture guard reproduced the regression before any code edits:

```text
python -m pytest tests/test_runtime_capability_architecture.py -q
1 failed, 3 passed in 0.53s
AssertionError: export_service.py grew to 1629 lines
```

Extracted cancellation/write controls, trusted disk row spooling, bounded page writing, and native batch streaming into `runtime/bounded_export.py`. A small `ExportFormatting` protocol describes the formatting and limit operations those helpers consume without importing the runtime service. `RuntimeExportService` retains its paged/streamed entry points and re-exports the existing runner/router control imports, preserving the shared cancellation context.

`export_service.py` now has 1,446 lines; `bounded_export.py` has 277. The architecture tests now include the extracted module in both the facade dependency check and the unchanged 1,500-line capability limit.

An AST comparison against the pre-extraction implementation verified equality for both moved writers, all five control/spool definitions, and every remaining service method, after ignoring added type annotations, the `self` to `service` collaborator rename, and docstring indentation.

Final focused verification:

```text
python -m pytest tests/test_runtime_capability_architecture.py tests/test_bounded_export_streaming.py tests/test_export.py tests/test_export_executor.py tests/test_export_qualification_runtime.py tests/test_runtime_export_job_runner.py tests/test_runtime_export_jobs.py tests/test_runtime_export_job_store.py tests/test_export_job_qualification.py tests/test_export_limits.py -q
139 passed, 14 skipped, 52 warnings in 6.63s
```

Exit status was 0. Warnings are the existing Ibis/DuckDB `fetch_arrow_table()` deprecations. Skipped cases retain their existing Redis-specific prerequisites/contracts; this run is not real-Redis release evidence. An earlier focused run also passed (133 passed, 14 skipped). `git diff --check` passed.

A bare full-suite run was started, then interrupted at the coordinating agent's instruction to avoid concurrent qualification. Its displayed progress had no failures before interruption; it produced no final summary and is not a full-suite pass. The coordinating Task 6 owns final full-suite and browser qualification. Other workers' fixture, browser, React test, and artifact edits were preserved.
