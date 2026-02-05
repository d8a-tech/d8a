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

## Go development rules (LLM)

This section embeds the former `coding.md` in its entirety (the standalone file was removed).

# Go development rules (LLM)


## Testing

- **Add tests for functionality you're implementing**
- **Start with designing interfaces so they're testable** - design interfaces and APIs with testability in mind from the beginning.
- **ALWAYS run tests for modules you're making changes in** - verify tests pass before considering work complete.
- **Always check the linter errors for changes you make** - fix them before declaring work complete.
- Prefer array-driven tests.
- Use `testify/assert`.
- Structure tests with `// given // when // then`.
- Use `testify.Mock` only when necessary; for small interfaces prefer simple hand-written mocks.
- Do not write tests that "cement" suspicious behavior. If you suspect logic is wrong, ask before encoding it into tests.
- **NEVER use hardcoded conditionals in test code** - if a test case needs different behavior, add fields to the test struct instead of checking test names or other hardcoded values.

## Interface and implementation pattern

- **Functions accept interfaces, implementations are unexported** - exported structs are for data, behaviors use interfaces.
- When defining behavior (methods), export the interface and keep the implementation struct unexported.
- Constructor functions return the interface type, not the concrete implementation.
- This pattern enables testability, allows for multiple implementations, and hides implementation details.
- Reference pattern: `src/integrations-api/pkg/oauthflow/state.go` (`StateSigner` interface, `stateSigner` unexported struct).
- There may be some exceptions, like for example *gorm.DB, gin ctx and other god objects from 3rd party libraries. 

## Error handling and logging

- **Avoid `logrus.Fatal` and `log.Fatal`** - these terminate the process immediately without allowing cleanup or proper error propagation.
- **Use `logrus.Panic` instead** for fatal errors that should terminate the application. Panics can be recovered by middleware and allow proper cleanup.
- Prefer returning errors when possible, especially in functions that can be tested or where callers should handle the error.
- For initialization failures in `Must*` functions or similar contexts where failure is unrecoverable, use `logrus.Panic`.
- Reference pattern: `src/libs/pkg/database/migrate.go` (`MustMigratedDb` uses `logrus.Panic`).

## Linting policy

- Only fix **simple** lint issues when asked (comments, unchecked errors, line length/formatting). The exception is your own just-written code.
- Do not refactor to satisfy complexity lints (mccabe/funlen/gocognit) unless explicitly requested.
- Exported methods added only to satisfy an interface: comment must be `// X implements package.Interface`.
- In **test code only**, it's acceptable to `//nolint` complexity/funlen rules.
- **Do not add `//nolint` comments UNLESS THE USER REQUESTS IT**
- Unchecked errors in `defer`: wrap in anonymous func and log via `logrus.Error`.

## CLI flags (urfave/cli)

- **Define flags as separate vars** - each flag should be a package-level variable.
- **Reference flags via the var's `.Name` property** - use `cmd.Duration(flagVarName.Name)`, not string constants like `cmd.Duration("flag-name")`.
- Reference pattern: `src/tracker-api/pkg/cmd/flags.go` (flag definitions) and `src/tracker-api/pkg/cmd/run.go` (flag usage).

Example:
```go
var serverPortFlag *cli.IntFlag = &cli.IntFlag{
    Name:    "server-port",
    Usage:   "Port to listen on for HTTP server",
    Sources: defaultSourceChain("SERVER_PORT"),
    Value:   8080,
}

// Later, in command action:
port := cmd.Int(serverPortFlag.Name)
```

## Filesystem cleanup

- **ALWAYS cleanup the filesystem after declaring work complete** - remove any binaries, temporary files, or artifacts created during development/testing.
- **When using `go build`, always specify `-o /tmp/whatever`** to avoid leaving binaries in the workspace (e.g., `go build -o /tmp/integrations-api .`).
- Before declaring work complete, verify no build artifacts remain in the workspace.

## Running tools (canonical commands)

- `go mod tidy`
- `go build -o /tmp/<service-name> .` (use `/tmp` to avoid leaving binaries in workspace)
- `go run . <command>`
- `go test ./... # optional args to select only a subset of tests`

Linting (from the same service directory):

- `~/go/bin/golangci-lint run ./... -c .golangci.yml`

## Docs style guide (applies to `docs/docs/**`)

- Headings start with a capital letter.
- Preserve product/company names (Google, Looker Studio) and database names (ClickHouse, BigQuery).
- Preserve acronyms (GA4, UTM, URL, etc.).
- Preserve step labels (e.g., `Step 1: Create a configuration file`).
- Otherwise use sentence case for the rest of the heading.

## Agent rules

This section contains tool-agnostic rules to follow.

### Linting

- If I ask you to fix linter errs, please fix only simple errors like lack of comment, unchecked error, lll (line lenght), etc - anything that can be solved with adding docstring and reformatting without changing logic 
- Complex thinkgs like mccabe, funlen, etc leave to me, unless I specifically ask you to fix given error. 
- If a method is exported just to satisfy the interface, the method comment should be just // X implements Y, where X is method, Y is the interface name (package.Interface)
- ONLY IN TEST CODE mccabe, cognitive complexity (gocoginit or somth) and funlen can be //nolinted
- If the linter complains about unchecked errors in deferred functions, wrap in anonymous func with logrus.Error
- You can check linter by ussing cd src/<project> && ~/go/bin/golangci-lint run ./..., where project is tracker-api, core-api, etc, depends on current context
- If you execute the linters for nth time, skip `cd src/<project>` - you're already in the directory, previous command got you there

### Testing

- When it's necessary, use testify.Mock, but if the interface is not a huge one, prefer writing own simple mocks
- When writing tests use array-driven approach, testify.assert and // given // when // then approach. 
- When writing tests - very important: do not blindly write tests to satisfy the current logic, if you suspect the current logic contains errors, that the tests should reveal, ask me for opinion. 

### Documenting

This instruction apply to to tasks, that require documenting the code.

- Make sure that the documentation is short, concise and to the point.
- The comment must bring information, that is not obvious from the documented entity name itself.
- Never hardcode into documentation assumptions about code, that is not present near the documented entity.

