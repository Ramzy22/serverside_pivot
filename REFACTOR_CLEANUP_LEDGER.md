# Refactor Cleanup Ledger

This ledger records desirable cleanup that is intentionally not hidden inside
the zero-break capability refactor. Removal requires separate approval or a
major-version compatibility decision.

| Item | Current action | Removal evidence / follow-up |
| --- | --- | --- |
| Private adapter/controller/runtime helper entry points | Keep explicit forwarding shims | Remove only after downstream and test call sites migrate |
| Legacy hierarchy strategies and managers | Isolate behind strategy adapters and profile usage | Remove strategies with no supported scenario or observed use |
| Duplicate generated package trees and bundles | Preserve canonical packaged assets during refactor | Define one build-output policy and prove wheel compatibility |
| Stray bundle under `pivot_engine/runtime` | Keep excluded from source control | Delete separately after confirming no packaging consumer |
| Node/npm engine declarations older than the build toolchain | Record supported build environment in CI | Change public engine metadata in a dedicated packaging release |
| Package and package-lock version drift | Regenerate lockfile when TypeScript dependencies are added | Add release-time version parity check |
| Manually enumerated Python subpackages | Add new capability packages explicitly for compatibility | Move to package discovery after wheel-content comparison |
| Dash metadata build failure masked in CI | Remove the failure mask during TypeScript bridge work | Keep strict generation as a permanent release gate |
| Source-contract tests coupled to implementation text | Retain until behavioral replacements exist | Remove only after equivalent TypeScript/Python behavior tests |
| Editing implementation guarded by a disabled flag | Preserve current user-visible behavior | Enable as a separately tested feature decision |
| Historical aliases and alternate payload spellings | Continue normalizing at boundaries | Deprecate and remove in a major release |
| Screenshots, temporary logs, and debug scripts | Exclude from refactor commits | Keep local or move deliberate fixtures into test assets |

