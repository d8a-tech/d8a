---
title: Switching from gtag.js
sidebar_position: 6
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide describes how to migrate an existing GA4 setup that uses `gtag()` and `dataLayer` to the d8a web tracker, while **keeping your existing `gtag()` calls**.

## Before you start

This approach works best when:

- Your site uses **`gtag('event', ...)`** (and optionally `gtag('set', ...)`, `gtag('consent', ...)`) to emit events.
- You can remove Google’s script-tag installation for GA4 (so there is no second `gtag()` implementation on the page).

:::warning
This is **not** a drop-in replacement for GTM-style pushes like `dataLayer.push({ event: '...' })`. The d8a web tracker consumes **array-like** items (snippet-style `arguments`), not plain objects (for example, `dataLayer.push(['event', 'purchase', { value: 123 }])`).
:::

## Step 1: Remove Google’s gtag installation

Remove the GA4 loading snippet from your page. It typically looks like this:

- The script that loads Google’s tag (example): `https://www.googletagmanager.com/gtag/js?...`
- The inline initialization snippet that defines `window.dataLayer` and `function gtag(){dataLayer.push(arguments)}`
- Any `gtag('config', 'G-XXXX', ...)` calls that configure Google Analytics

After this step, your site should no longer send events to Google Analytics.

## Step 2: Install d8a as `gtag()` using `dataLayer`

Install d8a using:

- `dataLayerName: 'dataLayer'` (to match the GA4 convention)
- `globalName: 'gtag'` (so your existing event calls keep working)

<Tabs>
<TabItem value="script-tag" label="Script tag" default>

<!-- prettier-ignore -->
```html
<script async src="https://global.t.d8a.tech/js?l=dataLayer&g=gtag"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  window.gtag = window.gtag || function(){dataLayer.push(arguments);};

  gtag('js', new Date());

  gtag('config', '<property_id>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
  });
</script>
```

</TabItem>
<TabItem value="npm-module" label="npm (module)">

<!-- prettier-ignore -->
```javascript
import { installD8a } from '@d8a-tech/web-tracker';

installD8a({ dataLayerName: 'dataLayer', globalName: 'gtag' });

const gtag = window.gtag;
if (!gtag) throw new Error('gtag is not installed');

gtag('js', new Date());
gtag('config', '<property_id>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
});
```

</TabItem>
</Tabs>

## Step 3: Keep your existing `gtag('event', ...)` calls

Once installed, your existing calls should continue to work (now sending to d8a), for example:

<!-- prettier-ignore -->
```javascript
gtag('event', 'purchase', {
  currency: 'USD',
  value: 149.97
});
```

## Notes

- **`server_container_url` is required**: without it, the tracker cannot build the final endpoint URL for sending.
- **Property IDs**: the first argument to `gtag('config', ...)` must be your **d8a** `<property_id>` (not a GA4 `G-XXXX` measurement ID).
- **Consent**: `gtag('consent', ...)` is supported.
