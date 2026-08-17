# Endpoint staged runtime ownership

**Goal:** Make endpoint prepare/commit/abort and shutdown share one staged-mutation owner so a successful database mutation cannot lose its runtime publication.
**Why planning is required:** This changes a public service/runtime contract and coordinates database state, listener ownership, shutdown, and concurrent lifecycle operations.
**Acceptance:** Prepare binds/builds before persistence; persistence failure aborts without runtime map/status side effects; persistence success commits exactly once; shutdown waits for in-flight stages before taking the final runtime snapshot; duplicate Commit/Abort and failed Prepare do not double-close or deadlock.

### Outcome 1: Capture deterministic regressions
- Work: Add controlled interleaving for Prepare → blocked persistence → Shutdown → persistence success → Commit, plus failed-bind state cleanliness and exact-once stage lifecycle checks.
- Verify: `go test ./cmd/resin ./internal/service -run 'Endpoint|endpoint' -count=1`

### Outcome 2: Unify staged ownership
- Work: Add an independent manager stage owner held from successful Prepare until Commit/Abort. Make Shutdown acquire it before setting stopping and snapshotting runtimes. Remove prepare-failure status publication.
- Verify: `go test ./cmd/resin ./internal/service -run 'Endpoint|endpoint' -count=20`

### Outcome 3: Audit and regression verification
- Work: Migrate test doubles to Prepare/Commit/Abort, retain legacy rollback failure as isolated evidence, and inspect the final endpoint call chain and diff.
- Verify: `gofmt -w ...`; `go test ./internal/service ./cmd/resin ./internal/proxy`; `go test ./... -count=1`; `go vet ./...`; `git diff --check`
