# Initial JavaScript Checking Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a strict check of the first six JavaScript modules mandatory for local builds, CI, and release builds without misrepresenting the remaining 128 files as checked.

**Architecture:** A dedicated TypeScript project explicitly lists checked JS modules. `npm run typecheck` runs both the existing TypeScript project and this JS project. A compiler-API probe injects a type error in memory into one admitted module and proves that the JS project rejects it. This is the first independently releasable batch of the broader migration; later batches have separate plans.

**Tech Stack:** TypeScript 5.8, Node.js, npm scripts, existing GitHub Actions build jobs.

**Spec:** `docs/superpowers/specs/2026-10-12-javascript-checking-design.md`

## Global Constraints

- Keep strict checking enabled for admitted JS files; no blanket `@ts-nocheck`, diagnostic baselines, or reduced strictness.
- Do not claim complete JS checking until all supported `src/lib` JavaScript is admitted and clean.
- Preserve the existing TypeScript check and the Dash, React, and Streamlit build commands.
- Keep release/deployment and shared-report qualification separate from this code-quality gate.

## Review Focus

- If a listed JS file has a new type error, `npm run typecheck` must fail. Task 1 runs the compiler probe.
- If the checked manifest is accidentally empty, the probe must fail. Task 1 asserts all six root files are present.
- If an imported dependency is checked transitively, its diagnostics must fail the JS project. Task 1 runs `tsc -p` on the full effective project.
- If CI or release bypasses the JS check, their existing `npm run build` must include the modified `typecheck` script. Task 2 inspects and tests both workflows.
- If a new JS file is added outside the manifest, documentation must still describe coverage as partial. Task 2 reports the checked and remaining counts.

---

### Task 1: Strict checked-JS project and non-vacuous gate

**Files:**
- Create: `dash_tanstack_pivot/tsconfig.checked-js.json`
- Create: `dash_tanstack_pivot/scripts/verify_checked_js_gate.mjs`
- Modify: `dash_tanstack_pivot/package.json`

**Interfaces:**
- Consumes: the six source files named in the spec and the installed `typescript` package.
- Produces: `npm run typecheck:js` and an expanded `npm run typecheck` that exits nonzero for a JS type error.

- [ ] **Step 1: Write the compiler probe first.** Load the dedicated config with the TypeScript API. Assert its root file set contains exactly the six spec-listed modules. Override the compiler host's `readFile` for `src/lib/utils/formatting.js` to append `/** @type {number} */ const __pivotJsGateProbe = 'bad';`. Compile and assert a TS2322 diagnostic from that file; exit nonzero otherwise.
- [ ] **Step 2: Run `node scripts/verify_checked_js_gate.mjs` before adding the config.** Expected: nonzero because the required JS project is absent.
- [ ] **Step 3: Add the dedicated config.** Extend `tsconfig.json`; set `checkJs: true`, retain `strict: true` and `noEmit: true`; override inherited `include` with an empty list; list `virtualAxes.js`, `formatting.js`, `Icons.js`, `useRuntimeExportState.js`, `useCellInteraction.js`, and `DrillThroughModal.js` as root `files`.
- [ ] **Step 4: Add npm scripts.** `typecheck:js` runs `tsc --project tsconfig.checked-js.json --noEmit` and the compiler probe. Existing `typecheck` runs the existing `tsc --noEmit` then `npm run typecheck:js`.
- [ ] **Step 5: Run `npm run typecheck` and the compiler probe.** Expected: exit 0, and the in-memory deliberate error is detected by the probe.
- [ ] **Step 6: Commit the config, probe, and package script changes.**

### Task 2: Coverage disclosure and host verification

**Files:**
- Modify: `docs/OPERATIONS.md`
- Test: `.github/workflows/ci.yml`, `.github/workflows/release.yml` through their `npm run build` steps (no workflow edits anticipated).

**Interfaces:**
- Consumes: `npm run typecheck:js` from Task 1.
- Produces: honest checked/unchecked scope documentation and build evidence for all three hosts.

- [ ] **Step 1: Record the six checked root files, 128 unchecked JS files, the command to run, and the rule for admitting a new file in `docs/OPERATIONS.md`.** Note that imported dependencies enter the effective compiler project and that this is not full-tree JS checking.
- [ ] **Step 2: Verify workflow paths.** Confirm both CI and release invoke `npm run build`, which invokes `npm run typecheck`; fail this task if either path bypasses the gate.
- [ ] **Step 3: Run `npm run typecheck`, `npm run build:js`, `npm run build:react`, and `npm run build:streamlit` from `dash_tanstack_pivot`.** Expected: all exit 0; preserve pre-existing untracked assets and identify any generated tracked changes.
- [ ] **Step 4: Run `python -m pytest tests -q --disable-warnings` from the repository root, then `git diff --check`.** Expected: zero failures and no whitespace errors.
- [ ] **Step 5: Commit the documentation and any required generated host assets.**

## Following batches

This plan intentionally ships only the first checked-JS batch. Subsequent plans must add the remaining 128 JS files in small domain-owned batches, resolve their real diagnostics, update checked/unchecked counts, and finish with the spec's full-tree check and release-bound host verification. No part of this plan permits calling the full migration complete.
