# AGENTS.md

This file is the single source of truth for AI coding agents working in this repository.

## Project overview

D8A is a GA4-compatible analytics pipeline written in Go. It ingests HTTP tracking requests, converts them into `hits.Hit` objects, queues them, groups them into proto-sessions, closes them into sessions, derives columns, and writes the resulting rows to a configured warehouse.

## Package layout and data flow

Core packages (see `docs/docs/articles/technical-deep-dive.md`):

- `pkg/receiver`: Receives HTTP requests, selects a `protocol.Protocol`, builds `hits.Hit`, and pushes hits into `receiver.Storage`.
- `pkg/worker`: Generic task system with `Publisher`, `Consumer`, `Task`, a `Worker` that maps task types to handlers, and optional middlewares.
- `pkg/bolt`: BoltDB-backed implementations for queue/storage primitives.
- `pkg/protosessions`: Consumes hit tasks, groups hits into proto-sessions, and closes them via a `protosessions.Closer`.
- `pkg/sessions`: Session closing, column execution, and writing to the warehouse (direct/in-place closing today).
- `pkg/columns` and `pkg/schema`: Column “interfaces” (what a column is) and column implementations (how it’s written) for event and session scope.
- `pkg/warehouse`: Warehouse abstractions and drivers (BigQuery, ClickHouse, etc.).

High-level pipeline:

- HTTP request → `receiver` creates `hits.Hit` → `receiver.Storage` (batching) → `worker.Publisher`
- `worker.Consumer` → `protosessions` groups by identifiers and timeouts → `protosessions.Closer`
- `sessions` runs columns + layout → `warehouse.Driver` writes rows

Critical invariant:

- Consecutive hits belonging to the same proto-session **must** be processed by the same worker (partitioning/isolation depend on this).

## Go development rules

These are the working defaults for changes in this repo.

## Testing

- Add tests for functionality you implement.
- Design APIs for testability (interfaces first).
- Run tests for modules you touch; keep them green.
- Prefer table-driven tests with `testify/assert` and `// given // when // then` structure.
- Prefer simple hand-written mocks; use `testify.Mock` only when it meaningfully reduces complexity.
- If behavior looks suspicious, ask before encoding it as a "golden" test.
- Never branch test logic on case names; add fields to the test case struct instead.

## Interfaces

- Functions accept interfaces; implementations are unexported.
- Export interfaces for behavior; keep exported structs primarily for data.
- Constructors return the interface type.
- Reference pattern: `src/integrations-api/pkg/oauthflow/state.go` (`StateSigner`, `stateSigner`).
- Exceptions are allowed for third-party "god objects" (e.g., `*gorm.DB`, gin context).

## Errors and logging

- Avoid `log.Fatal`/`logrus.Fatal`; prefer returning errors.
- For unrecoverable initialization failures (e.g., `Must*`), use `logrus.Panic`.
- If a deferred call can error, wrap it and log with `logrus.Error`.
- Reference pattern: `src/libs/pkg/database/migrate.go`.

## Linting

- Always fix lint issues in code you just wrote.
- When complexity lints (mccabe/funlen/gocognit) struck, propose the user a small refactor rather than just pulling a function out - it often makes code less readable.
- Exported methods added only to satisfy an interface: comment is `// X implements package.Interface`.
- `//nolint` is allowed only in test code for complexity/funlen, and only when explicitly requested.

Lint command (from the service directory): `~/go/bin/golangci-lint run ./... -c .golangci.yml`

## CLI flags (urfave/cli)

- Define flags as package-level vars.
- Read flags via `.Name` (e.g., `cmd.Duration(flagVar.Name)`), not string literals.
- Reference pattern: `src/tracker-api/pkg/cmd/flags.go` and `src/tracker-api/pkg/cmd/run.go`.

## Tooling and cleanup

- Canonical commands: `go mod tidy`, `go test ./...`, `go run . <command>`.
- When building locally, write binaries to `/tmp`: `go build -o /tmp/<service-name> .`.
- Before declaring work complete, ensure no build artifacts remain in the workspace.

## Documentation style

- Code comments: short, non-obvious, no assumptions not present nearby.
- `docs/docs/**` headings: start with a capital letter; preserve proper names/acronyms; keep step labels like `Step 1: ...`; otherwise use sentence case.
