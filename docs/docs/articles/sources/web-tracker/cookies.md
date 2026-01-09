---
title: Cookies
sidebar_position: 3
---

This page documents the cookies created by the d8a web tracker, their naming, and their value formats.

## Cookies created by the tracker

Unless consent explicitly denies analytics storage, the d8a web tracker creates and maintains:

- `_d8a`: Client ID cookie (shared across all d8a properties in the same cookie prefix scope).
- `_d8a_<property_id>`: Session context cookie (one per d8a property).

If `analytics_storage` consent is denied, the tracker does not write analytics cookies.

In that case, the tracker also avoids reading existing analytics cookies for identity and uses an in-memory identifier for the client ID that remains consistent for the duration of the single-page application's lifetime. This ensures the client ID does not change between page views in a single-page app, but will reset if the user reloads or leaves the application.

The tracker respects consent set via either source. When both are present, consent from `gtag('consent', ...)` (GTM/gtag) is preferred.

- `d8a('consent', ...)`
- `gtag('consent', ...)` (read from `window.dataLayer`)

If your gtag snippet uses a custom data layer name (gtag `l=`), configure it so the tracker can mirror consent updates:

- **Script tag**: pass `?gl=<queueName>` in the script `src` (or set `window.d8aGtagDataLayerName = '<queueName>'` before the tracker is installed).
- **npm (module)**: call `installD8a({ gtagDataLayerName: '<queueName>' })`.

## Cookie naming

Cookie names can be customized using `cookie_prefix` (via `d8a('config', ...)`).

## `_d8a` Client ID cookie

### Value format

The cookie value uses a d8a-specific prefix and two numeric parts:

`C1.1.<random_31bit_int>.<timestamp_seconds>`

The tracker reads this cookie to derive thess `cid` parameter as:

`<random_31bit_int>.<timestamp_seconds>`

## `_d8a_<property_id>` Session context cookie

### Value format

The session cookie is a `$`-delimited token list prefixed with `S1.1.`:

`S1.1.<token>$<token>$...`

Tokens are stored as `<key><value>` pairs, where `<key>` is a single character.

### Tokens

- `s`: Session id (timestamp seconds when the current session started).
- `o`: Session count (increments when a new session is created due to inactivity).
- `g`: Session engagement flag (0/1). This is sent as the `seg` parameter. The tracker flips it to `1` after the engaged time threshold is reached (`session_engagement_time_sec`, default 10).
- `t`: Last activity time (timestamp seconds of the last hit in the session).
- `j`: Per-session hit counter (increments for each hit in the same session).
- `d`: Opaque per-session identifier (random, URL-safe).

### Session rollover

The tracker creates a new session when `now - t` exceeds `session_timeout_ms` (default is 30 minutes). When a new session is created:

- `s` is set to the current time (seconds)
- `o` is incremented
- `g` is reset to `0`
- `t` is set to the current time (seconds)
- `j` is reset to `0`
- `d` is regenerated

## Cookie domain auto-selection

When `cookie_domain` is set to `"auto"` (the default), the tracker selects the broadest valid cookie domain by trying candidates from broadest to narrowest until the browser accepts one. This gtag-compatible behavior allows cookies to be shared across subdomains when possible.

For example, when the page hostname is `docs.d8a.tech`, the tracker tries domain candidates in this order:

1. `d8a.tech` (broadest — shared across all `*.d8a.tech` subdomains)
2. `docs.d8a.tech` (narrower — only for this subdomain)
3. `none` (host-only — no domain attribute, only for the exact hostname)

The tracker uses the first candidate that the browser accepts, enabling cookie sharing across subdomains when browser policies allow it. This is particularly useful for sites with multiple subdomains (for example, `www.example.com`, `docs.example.com`, `app.example.com`) where you want a consistent client ID across all subdomains.

## Cookie configuration options

For the full list of cookie-related settings, see the [Cookies section in the Configuration reference](configuration.md#cookies).
