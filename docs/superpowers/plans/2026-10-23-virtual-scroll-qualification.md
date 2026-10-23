# Virtual Scroll Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualify later row and pivot-column windows in Dash, React, and Streamlit against deterministic data and the pinned AG Grid 36.2 SSRM reference.

**Architecture:** Run a separate wide-data phase on the same installed artifacts used by other browser tests. Keep the small parity fixture for existing workflows. Capture actual V1 request/response IDs and DOM values for product cases, and SSRM requests and DOM values for reference cases; bind every record to the fixture and artifact hashes.

**Tech Stack:** Python, pytest, Selenium/Chrome, Arrow, V1 runtime, AG Grid 36.2 SSRM.

**Spec:** `tests/flagship_requirements.json` (`vertical-virtual-scroll.window`, `horizontal-virtual-scroll.window`); `docs/flagship-functionality-certification.md`.

## Global Constraints

- Editing is excluded from read-only parity.
- A required case cannot pass through a skip, a fabricated response, or a shared unit test alone.
- Keep the existing small parity fixture and its hash binding intact.
- Use a deterministic wide fixture exceeding two 50-row blocks and the initial pivot-column window.
- Exercise the same built wheel and npm tarball in all three hosts.

## Review Focus

- Row identity: duplicate or missing rows at a virtual block boundary must fail the browser assertion.
- Request correlation: a later DOM value from a stale earlier response must fail the test.
- Column identity: header and value from adjacent pivot columns must not be mistaken for the target.
- Host lifecycle: Streamlit reruns and iframe sizing must not hide a failed scroll.
- Reference scope: AG Grid may serve all pivot columns in one SSRM response; report this transport difference explicitly.

---

### Task 1: Deterministic wide scenario

**Files:**
- Create: `tests/data/flagship_scroll_rows.json`
- Modify: `examples/flagship_demo_data.py`
- Test: `tests/test_flagship_reference_fixture.py`

- [ ] Write a failing test for 120+ unique region groups, 24+ periods, literal first/last cell oracles, and consistent scenario precedence.
- [ ] Run the test and confirm it fails for the missing wide scenario.
- [ ] Add a reproducible generator and track its output; expose it only under a private test environment flag.
- [ ] Run the fixture tests and commit.

### Task 2: Product browser windows

**Files:**
- Modify: `tests/flagship_browser_support.py`
- Modify: `tests/test_flagship_workflows.py`
- Modify: `scripts/certify_flagship.py`
- Test: `tests/test_flagship_certification.py`

- [ ] Write a failing contract test for dedicated scroll-phase dispatch and complete host mapping.
- [ ] Run it to see the missing phase/mapping failure.
- [ ] Add product browser journeys with trusted scroll gestures, request/response correlation, row and column oracle assertions, return-to-first assertions, screenshots, and parsed traces.
- [ ] Run each host's installed-artifact cases, fix defects found, and commit.

### Task 3: Pinned AG Grid reference and release binding

**Files:**
- Modify: `reference/ag_grid_36_2/src/server.ts`
- Modify: `reference/ag_grid_36_2/src/main.ts`
- Modify: `tests/test_flagship_ag_grid_reference.py`
- Modify: `scripts/flagship_reference_runner.py`
- Modify: `tests/flagship_requirements.json`

- [ ] Write a failing reference test for the same wide scenario and oracle cells.
- [ ] Run it and confirm the missing scenario fails.
- [ ] Add scenario selection to the pinned reference, assert SSRM block requests and virtual DOM columns, and bind the wide fixture hash.
- [ ] Run focused reference and installed-product cases, then update only supported/verified contract fields.
- [ ] Run typecheck and the Python suite; commit the qualified result.
