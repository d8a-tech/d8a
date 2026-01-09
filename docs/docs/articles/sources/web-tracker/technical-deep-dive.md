---
title: Technical deep dive
sidebar_position: 6
draft: true
---

This document explains how the d8a web tracker works internally. It is intended for developers who want to debug, extend, or contribute to the tracker implementation.

## Overview

At a high level, the web tracker:

- Exposes a `gtag`-style API (`d8a('config' | 'event' | 'set' | 'consent', ...)`) through a global function and a queue.
- Consumes queued commands from a data layer array (default: `window.d8aLayer`).
- Converts commands into GA4 gtag-compatible `/g/collect` requests.
- Manages a small set of first-party cookies (when allowed by consent).
- Optionally enriches requests with User-Agent client hints (UA-CH) when available.
- Implements optional “enhanced measurement” (automatically generated events).

The runtime is intentionally dependency-light and ships as bundles built with esbuild.

## Runtime architecture

The runtime is composed of a few cooperating parts:

- **Global API (`d8a`)**: a small function that pushes calls into the data layer queue.
- **Queue consumer**: drains existing queued calls, patches `push()`, and routes commands to handlers (`config`, `event`, etc.).
- **Dispatcher**: batches events, resolves effective configuration (including precedence), and sends requests.
- **Protocol mapper**: maps event/context data into GA4-style query parameters.
- **Optional helpers**: consent bridge, enhanced measurement, cookie helpers.

## Control flow (from `d8a('event', ...)` to a network request)

1. The user calls `d8a('event', ...)` (or a call is already present in the queue).
2. The queue consumer observes the command and updates runtime state.
3. The dispatcher enqueues the event for sending.
4. On flush, the dispatcher:
   - selects destination property IDs (fan-out or `send_to` routing),
   - resolves cookie + consent behavior,
   - builds a `/g/collect` URL for each destination,
   - sends requests using `fetch(..., { keepalive: true, mode: 'no-cors' })` (with limited retries/backoff).

## Package structure

The web tracker source lives in `js/web-tracker/src/`. It is organized by responsibility (see each directory for the current, complete list of modules):

- **Entrypoints**: `src/index.js` (ESM export surface) and `src/browser_entry.js` (script-tag bundle entry).
- **Installer**: `src/install.js` wires together the runtime and exposes `installD8a`.
- **Runtime**: `src/runtime/` contains the in-browser runtime. The main “spine” is:
  - `src/runtime/queue_consumer.js` (reads commands and mutates runtime state)
  - `src/runtime/dispatcher.js` (batches and sends requests)
  - `src/runtime/state.js` (state shape + defaults)
- **GA4 mapping**: `src/ga4/` builds request payloads (for example `src/ga4/gtag_mapper.js`).
- **Cookies**: `src/cookies/` defines cookie formats and write behavior (for example `src/cookies/d8a_cookies.js`, `src/cookies/identity.js`).
- **Transport and utilities**: `src/transport/` and `src/utils/` contain send + URL helpers (for example `src/transport/send.js`, `src/utils/endpoint.js`).

## Developer experience

The tracker runtime is intentionally dependency-light. Most modules are written as small factories that accept injected dependencies (for example, `windowRef`) to keep code testable under Node and easy to debug.

### Browser compatibility

The published bundles target **ES2018** and rely on modern browser APIs like `fetch` and `URL`. This keeps the runtime small and avoids heavy transpilation.

### User-Agent client hints (UA-CH)
When available, the web tracker uses the UA-CH API (`navigator.userAgentData.getHighEntropyValues`) to enrich requests with higher-fidelity device and platform metadata. The implementation is feature-detected and cached per page load, and the tracker falls back to regular browser-derived behavior when UA-CH is not available (for example, in browsers that do not support the API).

### TypeScript support

The runtime source is **ESM JavaScript**, and the package ships **TypeScript type declarations** via `js/web-tracker/index.d.ts`.

### Why this structure

- **Low friction contributions**: plain ESM JavaScript + esbuild keeps the toolchain small.
- **Testability**: dependency injection (`windowRef`, `document`, etc.) enables deterministic tests without a real browser.
- **Debuggability**: sourcemaps are generated for the bundles.

## Build and test workflow

- **Build**: the package uses esbuild to produce minified bundles in `dist/` (script-tag bundle + ESM bundle).
- **Tests**: tests use Node’s built-in runner under `js/web-tracker/test/`.


