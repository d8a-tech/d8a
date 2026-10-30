---
title: Setup guide for AI coding agents
sidebar_position: 1
description: Step-by-step instructions for AI coding agents (Claude Code and others) to add d8a.tech analytics to a website, self-hosted or Cloud, with consent handling and key events.
---

# Setup guide for AI coding agents

This page is optimized for AI coding agents, such as Claude Code, that add analytics to a website on behalf of a person. If you are a person setting up d8a.tech yourself, start with [Getting started](/getting-started).

The result: the site sends events to d8a.tech (self-hosted or Cloud), key events are tracked in code, consent is respected, and the person can see their data in the d8a.tech dashboard or their own warehouse.

## What you need from the person

You need two values. Check the repository and the conversation first, and ask only for what is missing.

1. **Property ID**
2. **Tracking URL**, which depends on the setup and the protocol (see the table below)

If d8a.tech is not set up yet:

- **Cloud**: ask the person to create a free account at `https://app.d8a.tech/auth/signup`, then paste the Property ID shown by the setup wizard. Creating the account is the person's step, because they accept the terms. If you can use a browser on their behalf, you may open the page, but let them enter their details.
- **Self-hosted**: point the person to [Getting started](/getting-started). The Property ID is any string set as `property.id` in `config.yaml`. We suggest a lowercase UUID v4, for example from `uuidgen | tr A-Z a-z`.

## Tracking URL by protocol

Each Cloud property has one protocol, chosen when it was created. A self-hosted instance uses the `protocol` key in `config.yaml` (`d8a`, `ga4` or `matomo`). Use the endpoint that matches it.

| Protocol | Cloud | Self-hosted | Details |
| --- | --- | --- | --- |
| d8a native (default) | `https://global.t.d8a.tech/<property_id>/d/c` | `https://<host>/d/c` | [Web tracker](/articles/sources/web-tracker/) |
| GA4 duplication | `https://global.t.d8a.tech/<property_id>/g/collect` | `https://<host>/g/collect` | [Google Analytics 4](/articles/sources/ga4) |
| Matomo | `https://global.t.d8a.tech/<property_id>/matomo.php` | `https://<host>/matomo.php` | [Matomo](/articles/sources/matomo) |

In the snippets below, replace `<tracking_url>` with the URL from this table.

## Pick the install method

| The site today | Use | Why |
| --- | --- | --- |
| No analytics yet | Option A: the d8a web tracker | GA4-style API, no Google-hosted tags. |
| GA4 installed with `gtag` in the HTML | Option B: a second `gtag('config')` call | GA4 keeps working; d8a.tech receives a duplicate of every request. |
| GA4 installed through Google Tag Manager | The GA4 duplicator tag in GTM | Needs access to the GTM container. If you have none, give the person the steps from [Google Analytics 4](/articles/sources/ga4). |
| Matomo installed | A second Matomo tracker | See [Matomo](/articles/sources/matomo). |

Do not remove or change an existing GA4 or Matomo setup unless the person asks you to.

## Option A: d8a web tracker

Place this as high in the `<head>` as possible. Replace `<property_id>` and `<tracking_url>`.

```html
<script>
  window.d8aLayer = window.d8aLayer || [];
  window.d8a = window.d8a || function () { d8aLayer.push(arguments); };
  d8a('js', new Date());
  d8a('config', '<property_id>', {
    server_container_url: '<tracking_url>'
  });
</script>
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js"></script>
```

With npm instead:

```bash
npm install @d8a-tech/wt
```

```javascript
import { installD8a } from '@d8a-tech/wt';

installD8a();
const d8a = window.d8a;
d8a('js', new Date());
d8a('config', '<property_id>', {
  server_container_url: '<tracking_url>'
});
```

See [Web tracker](/articles/sources/web-tracker/) for all options.

## Option B: site already runs GA4 with gtag

Keep the existing `gtag('config', 'G-…')` call and add a second one. Use the `/g/collect` tracking URL.

```html
<script>
  // Existing GA4 destination, unchanged
  gtag('config', 'G-XXXXXXX');

  // Duplicate to d8a.tech
  gtag('config', '<property_id>', {
    service_container_url: '<tracking_url>'
  });
</script>
```

Events you send with `gtag('event', …)` now reach both GA4 and d8a.tech.

## Consent

Analytics consent is the site owner's decision. If the site has a consent banner, or serves visitors in the EU (recommended there), add a consent default before the `config` call:

```javascript
d8a('consent', 'default', { analytics_storage: 'denied' });
```

- When the visitor accepts, call `d8a('consent', 'update', { analytics_storage: 'granted' })` (or `gtag('consent', 'update', …)`). The d8a tracker also reads `gtag('consent', …)` from `dataLayer`, and prefers it when both are set.
- With `analytics_storage` denied, the d8a tracker writes no cookies and uses an in-memory client ID. See [Cookies](/articles/sources/web-tracker/cookies).
- If you are not sure what the owner wants, add the default and tell the person.

## Events

**Collected automatically** by the web tracker: `page_view`, `user_engagement`, outbound `click`, `file_download` and `view_search_results`. `session_start` and `first_visit` are derived on the backend. Do not send these yourself. See [Automatically collected events](/articles/sources/web-tracker/automatically-collected-events). With GA4 duplication, d8a.tech receives whatever GA4 already sends.

**Add in code** the business actions that matter, where the action succeeds, not with click listeners on CSS selectors. Use [GA4 recommended event names](https://support.google.com/analytics/answer/9267735) and parameters, so reports and attribution work without mapping:

| When | Event and key parameters |
| --- | --- |
| Account or newsletter sign-up | `sign_up`: `method` (e.g. `'email'`, `'newsletter'`) |
| Contact or demo form sent | `generate_lead`: `value`, `currency` |
| Item added to cart | `add_to_cart`: `items`, `value`, `currency` |
| Checkout started | `begin_checkout`: `items`, `value`, `currency` |
| Order completed | `purchase`: `transaction_id`, `value`, `currency`, `items` |

```javascript
// After the form submit succeeds, not on button click
d8a('event', 'generate_lead', { value: 0, currency: 'EUR' });
```

With Option B, use `gtag('event', …)` with the same names and parameters. Ask the person which actions count as conversions before adding more than these.

## Check that it works

1. Open the site with DevTools, Network tab, and filter by the tracking host.
2. Accept consent if there is a banner, then load a page and trigger each key event.
3. Each action should send a request to the tracking URL with the right `en` (event name) parameter.
4. Cloud: the person can watch hits arrive in debug mode in the d8a.tech app. Self-hosted: check the warehouse.

Data appears when each session closes, not instantly.

## Tell the person what you did

End with a short summary:

- The install method and where the snippet lives in the code.
- The key events you added and the file for each one.
- How consent is handled.
- Where to see the data: the d8a.tech dashboard, or their warehouse.
- Cloud only: the plan and its monthly event limit (Cloud Dashboard: 500k free, then $20 to $1,000/month by volume, custom quote above 20M events; Cloud Warehouse: 30M free).

## Next: ask AI about the data (Cloud only)

With d8a.tech Cloud, the person can connect the read-only [MCP connector](/articles/mcp) so Claude and other MCP clients can read reports, explain what converts, and audit the tracking you installed. It is not available for self-hosted d8a.tech.
