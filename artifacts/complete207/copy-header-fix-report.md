# Copy Selection with Headers investigation

qualificationEvidence: false

Candidate2 Streamlit body copy succeeds with exact bytes `3\t50\r\n2\t100`; the second native context click never exposes the requested headers action. No production fix has been selected without event evidence.

Source findings: ContextMenu invokes the action and closes synchronously; the asynchronous rich clipboard notification does not itself close context state. The global document click handler does close context state. ContextMenu focuses its first item using focus() with scrolling allowed. The failing screenshot places the selected first row near the bottom of the viewport. These are investigation boundaries, not a proven root cause.

Added a read-only diagnostic observer around both native copy opens. It records trusted pointer/mouse/context/click/focus events, composed paths across shadow roots, post-event prevention state, active control, menu geometry/items and menu mutations. Checkpoints include target geometry, deep shadow hit target and viewport. The trace is bounded at 160 entries and written to the existing private observation on the menu timeout. No event is canceled, intercepted, synthesized or replayed; native ActionChains context_click and native menu click remain unchanged. Clipboard literals, order, headers and selection assertions are unchanged.

Validation: python -m pytest tests/test_selection_context_probe.py tests/test_flagship_copy_display_order.py -q — 7 passed. Scoped tests prove failure retains first/second-open trace and success still clicks the exact headers action and rejects wrong clipboard body order.

Pending: root-coordinated native diagnostic reproduction against the existing candidate2 host; then choose and validate a minimal fix from the observed cause. No browser was launched by this worker.
