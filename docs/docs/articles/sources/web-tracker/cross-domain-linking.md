---
title: Cross-domain linking
sidebar_position: 4
---

Use cross-domain linking when a user journey spans **multiple domains that do not share a cookie base domain** (for example, `store.example` → `example-checkout.com`). The d8a web tracker can preserve client and session continuity by decorating outbound URLs with a short-lived `_dl` parameter.

## Installation

Cross-domain linking requires the web tracker to be installed on **both** the source domain and the destination domain.

Follow the [Web tracker quick start](index.md#quick-start) on each domain first, then configure the linker.

## Configuration

Configure the linker using `set('linker', ...)`:

<!-- prettier-ignore -->
```javascript
// Decorate links that point to these domains.
d8a('set', 'linker', {
  domains: ['example-checkout.com'],
  // Optional:
  // decorate_forms: true,
  // url_position: 'query', // default
  // accept_incoming: true, // default when domains is non-empty
});
```

Options:

- **`domains`** (required): Destination domains that should be decorated when a link or form submits to a matching hostname.
- **`decorate_forms`** (optional, default: `false`): When enabled, the tracker also decorates form submissions.
- **`url_position`** (optional, default: `'query'`): Where to place `_dl` (`'query'` or `'fragment'`).
  - Example (`'query'`): `https://example-checkout.com/checkout?_dl=<payload>`
  - Example (`'fragment'`): `https://example-checkout.com/checkout#_dl=<payload>`
- **`accept_incoming`** (optional): Whether the destination site should accept incoming `_dl`.
  - Default: enabled when `domains` is non-empty.
  - If you only want to **accept** `_dl` (but not decorate outbound links), set `domains: []` and `accept_incoming: true`.

Cookie overwrite behavior is controlled by `cookie_update` (see [Configuration](configuration.md#cookies)):

- If `cookie_update=false`, existing cookies are **not overwritten**, but missing cookies can still be created.

## How it works (technical overview)

At a high level:

- **Outbound**: When the user clicks an outbound `<a>` (and optionally submits a form), the tracker checks whether the destination hostname matches `linker.domains`. If so, it appends a `_dl` parameter.
  - If multiple tracker instances (`d8a1`, `d8a2`, …) run on the same page, they share one linker and still produce **a single** `_dl` value. The payload includes cookies for **all configured properties / cookie prefixes** across those instances.
  - Decoration is performed lazily on user interaction by listening to `document` events:
    - `mousedown` and `keyup` for links, and
    - `submit` for forms.
      Additionally, programmatic form submissions are handled by patching `HTMLFormElement.prototype.submit`.
- **Payload**: `_dl` carries a compact, URL-safe encoding of the **serialized d8a cookies** (client + session cookies). The payload is:
  - **short-lived** (about 2 minutes), and
  - protected by a **non-secret fingerprint/hash** (UA + timezone + language + time window + payload) to reduce accidental corruption.
- **Inbound**: On the destination domain, the tracker reads `_dl` from the URL, validates it, and then **strips `_dl` from the URL** (using `history.replaceState`) to keep the URL clean. Stripping happens regardless of whether validation succeeds.
- **Applying identity**:
  - If `analytics_storage` consent is denied, the tracker does not write cookies and instead keeps the incoming client id in memory for sending hits.
  - If consent allows cookies, the tracker writes incoming cookies using the current cookie settings (domain/path/prefix/etc.) and respects `cookie_update`.
  - If not enough configuration is available yet to recognize the cookies contained in `_dl` (for example, before property cookie prefixes become known), the tracker keeps the incoming payload and applies cookies **incrementally** as configuration becomes available.
  - When an incoming **session** cookie is applied, the tracker also seeds the in-memory session state so that an immediate `page_view` does not accidentally start a new session.
