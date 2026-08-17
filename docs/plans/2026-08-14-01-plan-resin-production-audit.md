# Resin production-path audit continuation

**Goal:** Continue the current-worktree audit by proving and fixing each newly reproducible production-path defect without disturbing existing user changes.
**Why planning is required:** This is a high-risk concurrency, lifecycle, and persistence audit with cross-package state ownership.
**Acceptance:** Every selected candidate has a deterministic old-behavior red test, a root-cause fix at the owning boundary, focused and affected-call-chain evidence, and no deployment, commit, push, remote access, or cleanup.

### Outcome 1: Select the next executable candidate
- Work: Inspect current diff and production callers for an unclosed worker, persistence, or resource-lifecycle boundary; preserve `resin-readcheck.exe` and unrelated changes.
- Verify: Read-only `rg`/file inspection plus a documented falsifiable hypothesis.

### Outcome 2: Capture and repair one root cause
- Work: Add a deterministic interleaving regression, confirm the old path fails, then apply the smallest owner/admission/ordering fix at the demonstrated source and cover exceptional paths.
- Verify: Focused red evidence followed by focused green repetition and affected package tests.

### Outcome 3: Audit and verify the resulting chain
- Work: Re-read all real callers and final diff for the changed boundary; remove temporary instrumentation and keep user files intact.
- Verify: `go test` for affected packages, `go vet` for affected packages, `go test ./...`, `go vet ./...`, and `git diff --check` with actual exit codes.
