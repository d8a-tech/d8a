# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

D8A is a GA4-compatible analytics platform built in Go. The system ingests analytics hits from clients, processes them through a pipeline of event handling and session management, and outputs structured data for analysis. The architecture follows a modular design with clear separation of concerns between data ingestion, processing, storage, and export.

## Development Commands

### Running the Server
```bash
go run main.go server
```

### Testing
```bash
go test ./...
```

### Building
```bash
go build -o d8a main.go
```

## Architecture

### Core Components

**Protocol Layer** (`pkg/protocol/`): Handles different analytics protocols. The GA4 protocol implementation (`pkg/protocol/ga4/`) parses Google Analytics 4 requests and converts them into internal hit structures.

**Receiver** (`pkg/receiver/`): HTTP server that accepts incoming analytics requests, validates them, and forwards to storage. Uses fasthttp for performance.

**Hit Processing Pipeline**: 
- Hits are received and stored via `Storage` interface
- Background workers consume hits from storage queues
- Hits are processed through configurable middleware chains
- Session management groups related hits together

**Session Management** (`pkg/sessions/`, `pkg/protosessions/`):
- Proto-sessions collect individual hits for a user session
- Configurable session timeout and closing triggers
- Sessions are closed and written to warehouse when complete

**Storage Abstraction** (`pkg/storage/`): Provides KV (key-value) and Set interfaces with implementations for in-memory and BoltDB backends.

**Column System** (`pkg/columns/`): Event and session columns define what data is extracted from hits and how it's structured for output. Columns are organized into event-scoped and session-scoped categories.

**Warehouse Integration** (`pkg/warehouse/`): Abstracted drivers for outputting processed data. Currently includes console output driver.

**Currency Conversion** (`pkg/currency/`): Provides currency conversion interface with banker's rounding for financial calculations.

### Data Flow

1. HTTP requests arrive at receiver endpoints (e.g., `/g/collect` for GA4)
2. Protocol parsers extract structured data from requests into Hit objects
3. Hits are validated and stored in queues via Storage adapters
4. Background workers consume hits and group them into proto-sessions
5. Session closing logic determines when to finalize sessions
6. Complete sessions are written to warehouse drivers for output
7. Column extractors transform hit/session data into structured output format

### Key Interfaces

- `protocol.Protocol`: Defines how to parse incoming requests into hits
- `Storage`: Abstracts hit queuing and retrieval
- `storage.KV` and `storage.Set`: Low-level storage primitives
- `SessionWriter`: Handles writing complete sessions to output
- `schema.EventColumn` and `schema.SessionColumn`: Define data extraction logic

### Configuration

The server accepts various CLI flags for configuration:
- `--server-port`: HTTP server port (default: 8080)
- `--batcher-batch-size`: Batch size for processing (default: 5000)
- `--batcher-batch-timeout`: Batch timeout (default: 5s)
- `--closer-session-timeout`: Session timeout before auto-close (default: 1m)
- `--closer-tick-interval`: Session cleanup frequency (default: 1s)

All flags can also be set via environment variables (e.g., `SERVER_PORT`, `BATCHER_BATCH_SIZE`).

## Code Standards

### Linting Guidelines
- Fix simple linting errors like missing comments, unchecked errors, and line length (lll)
- For exported methods that only satisfy interfaces, use comment format: `// MethodName implements InterfaceName`
- Complex issues (mccabe, funlen) should be left for manual review unless specifically requested
- In test code only, complex linting issues can be `//nolint`ed
- Wrap unchecked errors in deferred functions with anonymous functions using `logrus.Error`
- Run linter with: `~/go/bin/golangci-lint run ./...`

### Testing Standards  
- Use array-driven test approach with testify.assert
- Structure tests with `// given // when // then` comments
- Prefer simple custom mocks over testify.Mock for small interfaces
- Don't write tests that blindly follow current logic - if logic seems incorrect, seek clarification first