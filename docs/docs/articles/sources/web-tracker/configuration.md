---
title: Configuration
sidebar_position: 2
---

This page lists all supported configuration options for the d8a web tracker.

Configuration can be provided via:

- `d8a('config', '<property_id>', { ... })`: Per-property configuration.
- `d8a('set', { ... })`: Global defaults applied to subsequent hits (and used as fallback when a property config does not provide a value).
- `d8a('set', '<field>', <value>)`: Single-field global defaults (equivalent to the object form).

When the same key is provided in multiple places, values are resolved as:

- Event params > config params > set params > browser defaults

## Data collection

These options control where events are sent and how they are batched.

- `server_container_url` (required): Tracking URL for a property (the tracker uses this as the final endpoint).
  - Example (cloud): `https://global.t.d8a.tech/80e1d6d0-560d-419f-ac2a-fe9281e93386/d/c`
  - Example (on-prem): `https://example.org/d/c`
- `max_batch_size` (optional, default: `25`): Maximum number of queued events sent in a single flush. If the queue reaches this size, the tracker flushes immediately (no waiting for `flush_interval_ms`).
- `flush_interval_ms` (optional, default: `1000`): Time-based flush interval used when the queue is not full. If there are pending events after a flush, the tracker schedules another flush after this delay.

## Cookies

The tracker manages two first-party cookies:

- `_d8a`: Client ID cookie
- `_d8a_<property_id>`: Session context cookie

For the full cookie structure and examples, see [Cookies](cookies.md).

Cookie options:

- `cookie_domain` (optional, default: `"auto"`): Cookie domain strategy. When set to `"auto"`, the tracker automatically selects the broadest valid domain by trying candidates from broadest to narrowest (see [Cookie domain auto-selection](cookies.md#cookie-domain-auto-selection)). Set to `"none"` for host-only cookies (no domain attribute), or provide an explicit domain string (for example: `"example.com"`).
- `cookie_path` (optional, default: `"/"`): Cookie path.
- `cookie_expires` (optional): Cookie lifetime in seconds. If not provided, the tracker uses a GA4-like default of 2 years.
- `cookie_prefix` (optional, default: `""`): Prefix applied to cookie names (useful for isolating identities between trackers).
- `cookie_update` (optional, default: `true`): Whether the tracker refreshes cookie expirations on activity. Note: security-related attribute updates (SameSite/Secure/etc.) and creating missing cookies may still require a write.
- `cookie_flags` (optional): Raw cookie flags string (for example: `SameSite=Strict;Secure`).

## Cross-domain linker

The tracker can pass client and session context between **different domains** by decorating outbound links (and optionally forms) with a short-lived `_dl` parameter and accepting incoming `_dl` on the destination domain.

For the full guide (including technical details), see [Cross-domain linking](cross-domain-linking.md).

Configuration is set via:

- `d8a('set', 'linker', { ... })`

Options:

- `linker.domains` (required): Array of destination domains. When a link (or form) targets a hostname that matches one of these strings (substring match), the tracker decorates the URL.
- `linker.accept_incoming` (optional): Whether to accept incoming `_dl` on the current page.
  - Default: `true` when `linker.domains` is non-empty, otherwise `false`.
- `linker.decorate_forms` (optional, default: `false`): When enabled, decorate form submissions too.
- `linker.url_position` (optional, default: `'query'`): Where `_dl` is placed (`'query'` or `'fragment'`).

Cookie write behavior on the destination domain respects consent and `cookie_update`:

- If `analytics_storage` consent is denied, cookies are not written.
- If `cookie_update=false`, existing cookies are not overwritten, but missing cookies can still be created (gtag-like).

## Debugging

- `debug_mode` (optional): Enables debug logging and adds `_dbg=1` and `ep.debug_mode=1` to tracking requests.

## Identity and user fields

- `user_id` (optional): Sets the user identifier sent with tracking requests (stored in memory by the tracker).
- `client_id` (optional): Overrides the client identifier sent with tracking requests (instead of the value derived from the client ID cookie).

## Campaign and page overrides


- `campaign_id`
- `campaign_source`
- `campaign_medium`
- `campaign_name`
- `campaign_term`
- `campaign_content`
- `page_location`
- `page_title`
- `page_referrer`
- `content_group`
- `language`
- `screen_resolution`
- `ignore_referrer`

## Page view behavior

- `send_page_view` (optional, default: `true`): When `true`, each `config` call automatically triggers a `page_view` event for that property. Set to `false` to disable automatic page views.

## Enhanced measurement

- `site_search_enabled` (optional, default: `true`): Enables site search auto-capture.
- `site_search_query_params` (optional, default: `"q,s,search,query,keyword"`): Search query parameter keys (CSV string or string array).
- `outbound_clicks_enabled` (optional, default: `true`): Enables outbound click auto-capture.
- `outbound_exclude_domains` (optional): Domains excluded from outbound tracking (CSV string or string array). Defaults to the current site hostname.
- `file_downloads_enabled` (optional, default: `true`): Enables file download auto-capture.
- `file_download_extensions` (optional): File extensions considered downloads (CSV string or string array). Defaults to: `pdf,doc,docx,xls,xlsx,ppt,pptx,csv,txt,rtf,zip,rar,7z,dmg,exe,apk`.

## Engagement

Engagement-related options:

- `session_engagement_time_sec` (optional, default: `10`): Minimum engaged time (in seconds) required to flip `seg=1` for the session.
- `session_timeout_ms` (optional, default: `1800000`): Session timeout window used for the Session context cookie. Note: This affects only the web tracker’s client-side session state (for example, the `session_id` it sends). D8a also calculates sessions on the backend.


