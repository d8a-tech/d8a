---
title: Web tracker
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The d8a web tracker provides a GA4 gtag-style API that sends GA4 gtag-compatible requests directly to a d8a collector. Use it when you want GA4-style tracking without relying on Google-hosted tags.

:::caution Beta
The web tracker is currently in **beta** - feel free to report any problems via [GitHub Issues](https://github.com/d8a-tech/d8a/issues).
:::

## Quick start

<Tabs>
<TabItem value="script-tag" label="Script tag" default>
Add the snippet to your page:
<!-- prettier-ignore -->
```html
<script async src="https://global.t.d8a.tech/js"></script>
<script>
  window.d8aLayer = window.d8aLayer || [];
  window.d8a = window.d8a || function(){d8aLayer.push(arguments);};

d8a('js', new Date());
d8a('config', '<property_id>', {
server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
});
</script>

````

</TabItem>
<TabItem value="npm-module" label="npm (module)">

First, install the package:

```bash
npm install @d8a-tech/web-tracker
````

Then use the following code to initialize:

<!-- prettier-ignore -->
```javascript
import { installD8a } from '@d8a-tech/web-tracker';

installD8a();

const d8a = window.d8a;
if (!d8a) throw new Error('d8a is not installed');

d8a('js', new Date());
d8a('config', '<property_id>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
});
```

</TabItem>
</Tabs>

::::note
By default, `send_page_view` is enabled, so the tracker will send a `page_view`. Enhanced measurement is also enabled by default (see [Enhanced measurement](#enhanced-measurement)). The `d8a()` function also exposes convenience methods (`d8a.config`, `d8a.event`, `d8a.set`, `d8a.consent`) that behave the same as the string-command form.
::::

## Configuration

- `server_container_url`: Required. Tracking URL for this property (the tracker uses this as the final endpoint).
  - Example (d8a Cloud): `https://global.t.d8a.tech/80e1d6d0-560d-419f-ac2a-fe9281e93386/d/c`
  - Example (Self-Hosted): `https://example.org/d/c`
- For all available options, see [Configuration](configuration.md).

## Events

The tracker supports sending events using `d8a('event', '<event_name>', { ... })`. Here are some of the most commonly used events:

### Page view

Automatically sent when a property is configured (unless `send_page_view: false`). You can also send it manually:

<!-- prettier-ignore -->
```javascript
d8a('event', 'page_view', {
  page_title: 'Homepage',
  page_location: 'https://example.com/',
});
```

### Add to cart

Track when users add items to their shopping cart:

<!-- prettier-ignore -->
```javascript
d8a('event', 'add_to_cart', {
  currency: 'USD',
  value: 29.99,
  items: [
    {
      item_id: 'SKU_123',
      item_name: 'Product Name',
      price: 29.99,
      quantity: 1,
    },
  ],
});
```

### Purchase

Track completed purchases:

<!-- prettier-ignore -->
```javascript
d8a('event', 'purchase', {
  transaction_id: 'T_789456',
  value: 149.97,
  currency: 'USD',
  tax: 12.0,
  shipping: 9.99,
  coupon: 'WELCOME10',
  items: [
    {
      item_id: 'PROD_001',
      item_name: 'Classic Blue Denim Jeans',
      item_brand: 'FashionCo',
      item_category: 'Apparel',
      item_category2: 'Men',
      item_category3: 'Pants',
      item_variant: 'Blue',
      price: 79.99,
      quantity: 1,
      discount: 8.0,
      coupon: 'WELCOME10',
      index: 0,
    },
    {
      item_id: 'PROD_002',
      item_name: 'Wireless Bluetooth Headphones',
      item_brand: 'TechSound',
      item_category: 'Electronics',
      item_category2: 'Audio',
      item_category3: 'Headphones',
      item_variant: 'Black',
      price: 59.99,
      quantity: 1,
      discount: 0,
      index: 1,
    },
  ],
});
```

### Custom events

You can send any custom event name:

<!-- prettier-ignore -->
```javascript
d8a('event', 'newsletter_signup', {
  newsletter_type: 'weekly',
  source: 'homepage',
});
```

For a complete list of recommended events and their parameters, see the [GA4 recommended events documentation](https://developers.google.com/analytics/devguides/collection/ga4/reference/events).

## Enhanced measurement

The tracker can automatically capture GA4-style events (enabled by default):

- **Site search**: Fires `view_search_results` when the URL query string contains a known search parameter.
  - Default keys: `q,s,search,query,keyword`
- **Outbound clicks**: Fires `click` with `link_url`, `link_domain`, `outbound=1`.
- **File downloads**: Fires `file_download` with `link_url`, `file_name`, `file_extension`.

You can configure (or disable) these via `d8a('config', ...)` or `d8a('set', ...)`. For all available configuration options, see [Enhanced measurement settings](configuration.md#enhanced-measurement).

Example (disable all auto events):

<!-- prettier-ignore -->
```html
<script>
  d8a('config', '<property_id>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c',
    site_search_enabled: false,
    outbound_clicks_enabled: false,
    file_downloads_enabled: false,
  });
</script>
```

:::::note
Enhanced measurement is intentionally lightweight:

- Site search triggers at most once per page load (no SPA tracking beyond that).
- Click-driven enhanced measurement events flush promptly to reduce drop-off on navigation.
- The tracker does **not** support automatic SPA page views, scroll tracking, form interactions, or video engagement. The default `gtag.js` implementations for these are often too generic or unreliable for production needs. We recommend implementing these via a Tag Manager (like GTM) or custom event listeners for precise control.
  :::::

## Cookies

The tracker manages two first-party cookies (unless consent explicitly denies analytics storage):

- `_d8a`: Client ID cookie (value format: `C1.1.<a>.<b>`).
- `_d8a_<property_id>`: Session context cookie (value format: `S1.1...`).

For details, see [Cookies](cookies.md).

## Engagement

The tracker emits `user_engagement` events to tell the backend whether the user was actually engaged.

When `analytics_storage` consent changes after at least one property is configured, the tracker emits a lightweight `user_engagement` ping marked with `ep.consent_update=1`.

Each event includes `_et` (engagement time in milliseconds): time accumulated only while the page is active, visible, and focused.

When the accumulated `_et` for the current session reaches the engagement threshold (default: 10 seconds, configurable via `session_engagement_time_sec`), the tracker updates the Session context cookie to mark the session as engaged. After that, subsequent events from that session include `seg=1`.

## Related docs

- [GA4 gtag protocol reference](/articles/tracking-protocols/ga4-gtag)
