---
title: Setup guide for AI coding agents
sidebar_position: 1
description: Step-by-step instructions for AI coding agents (Claude Code and others) to add d8a.tech analytics to a website, with consent handling and key events.
---

# Setup guide for AI coding agents

This page is written for AI coding agents, such as Claude Code, that add analytics to a website on behalf of a person. People can follow it too.

The result: the site sends GA4-compatible events to d8a.tech, key events are tracked in code, consent is respected, and the person can see their data in the d8a.tech dashboard or their own warehouse.

## What you need from the person

You need one value: the **Property ID** of a d8a.tech Cloud property.

Creating the account is the person's step, because they accept the terms. Ask once, in one sentence, for example: "Create a free account at app.d8a.tech/auth/signup, then paste me your Property ID." If you can use a browser on the person's behalf (for example in Claude Cowork), you may open the sign-up page for them, but let them enter their details and accept the terms.

What the person sees after signing up:

1. The setup wizard creates a property. By default the data goes to the managed database with the built-in dashboard (Cloud Dashboard, free up to 500k events a month).
2. To send the data to their own BigQuery instead (Cloud Warehouse, free up to 30M events a month), they connect it in the wizard. That takes a few more steps; you can't do it for them.
3. They copy the Property ID and paste it to you.

Self-hosted d8a.tech works the same way, but the tracking URL is the person's own host. See [Getting started](/getting-started).

## Pick the install method

| The site today | Use | Why |
| --- | --- | --- |
| No analytics yet | The d8a web tracker | GA4-style API, no Google-hosted tags. |
| GA4 installed with `gtag` in the HTML | A second `gtag('config')` call | GA4 keeps working; d8a.tech receives a duplicate of every request. |
| GA4 installed through Google Tag Manager | The GA4 duplicator tag in GTM | Needs access to the GTM container. If you have none, give the person the steps from [Google Analytics 4](/articles/sources/ga4). |

Do not remove or change an existing GA4 setup unless the person asks you to.

## Option A: d8a web tracker (new sites)

Place this as high in the `<head>` as possible. Replace `<property_id>` in both places.

```html
<script>
  window.d8aLayer = window.d8aLayer || [];
  window.d8a = window.d8a || function () { d8aLayer.push(arguments); };

  // Consent default: wait for the site's consent banner.
  d8a('consent', 'default', { analytics_storage: 'denied' });

  d8a('js', new Date());
  d8a('config', '<property_id>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
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
d8a('consent', 'default', { analytics_storage: 'denied' });
d8a('js', new Date());
d8a('config', '<property_id>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
});
```

A `page_view` is sent automatically, and enhanced measurement is on by default. See [Web tracker](/articles/sources/web-tracker/) for all options.

## Option B: site already runs GA4 with gtag

Keep the existing `gtag('config', 'G-…')` call and add a second one:

```html
<script>
  // Existing GA4 destination, unchanged
  gtag('config', 'G-XXXXXXX');

  // Duplicate to d8a.tech
  gtag('config', '<property_id>', {
    service_container_url: 'https://global.t.d8a.tech/<property_id>/g/collect'
  });
</script>
```

Events you send with `gtag('event', …)` now reach both GA4 and d8a.tech.

## Consent

Analytics consent is the site owner's decision and depends on their legal basis. Follow their setup; never load tags around a consent banner.

- If the site has a consent banner, call `d8a('consent', 'update', { analytics_storage: 'granted' })` (or `gtag('consent', 'update', …)`) when the visitor accepts. The d8a tracker also reads `gtag('consent', …)` from `dataLayer`, and prefers it when both are set.
- With `analytics_storage` denied, the d8a tracker writes no cookies and uses an in-memory client ID. See [Cookies](/articles/sources/web-tracker/cookies).
- If you are not sure what the owner wants, load the tracker only after consent is granted, and tell the person what you did.

## Key events

Add events where the action happens in code, not with click listeners on CSS selectors. Prefer GA4 recommended event names, so reports and attribution work without mapping.

| Action on the site | Event name | Useful parameters |
| --- | --- | --- |
| Account created | `sign_up` | `method` |
| Contact or demo form sent | `generate_lead` | `form_id`, `value`, `currency` |
| Newsletter sign-up | `sign_up` | `method: 'newsletter'` |
| Item added to cart | `add_to_cart` | `currency`, `value`, `items` |
| Order completed | `purchase` | `transaction_id`, `value`, `currency`, `items` |

```javascript
// After the form submit succeeds, not on button click
d8a('event', 'generate_lead', { form_id: 'contact', value: 0, currency: 'EUR' });
```

With Option B, use `gtag('event', …)` with the same names and parameters.

Ask the person which actions count as conversions before adding more than these.

## Check that it works

1. Open the site with DevTools, Network tab, and filter by `d8a.tech` (or `collect`).
2. Accept consent if there is a banner, then load a page and trigger each key event.
3. Each action should send one request to `global.t.d8a.tech/<property_id>/…` with the right `en` (event name) parameter.
4. To watch hits arrive as they happen, the person can open debug mode in the d8a.tech app in another tab.

Data appears in d8a.tech when each session closes, not instantly.

## Tell the person what you did

End with a short summary:

- The plan they are on and its monthly event limit (Cloud Dashboard: 500k free, then $20 to $1,000/month by volume, custom quote above 20M events; Cloud Warehouse: 30M free).
- The install method and where the snippet lives in the code.
- The key events you added and the file for each one.
- How consent is handled.
- Where to see the data: the d8a.tech dashboard, or their warehouse.

## Next: let the data improve the site

d8a.tech has a read-only MCP server for Claude, in our cloud and self-hosted. Once data arrives, the person can connect it so Claude can read what converts and propose changes. Read-only means you can query the data, never change it. See [AI-ready analytics](https://d8a.tech/ai-ready-analytics/).
