---
title: Matomo
sidebar_position: 2
---

This guide shows two ways to use the Matomo tracker with d8a:

- duplicate tracking to both Matomo and d8a (multiple trackers),
- or fully replace Matomo destination with d8a using the Matomo protocol.

Source concept: [Matomo - Multiple Matomo trackers](https://developer.matomo.org/guides/tracking-javascript-guide#multiple-piwik-trackers).

:::warning
Matomo protocol ingestion is currently supported only in d8a OSS. Cloud support is coming soon.
:::

## Option 1: Multiple trackers (keep Matomo and send to d8a)

Use this setup when:

- your site already runs the Matomo JavaScript tracker,
- you want to keep current Matomo reporting,
- and you want the same events in d8a.

## Setup

1. Keep your existing Matomo tracker config (`setTrackerUrl`, `setSiteId`) unchanged.
2. Add one `addTracker` call pointing to your d8a Matomo-compatible endpoint.
3. Keep normal tracking calls (`trackPageView`, `trackEvent`, etc.) as-is.

```html
<script>
  var _paq = (window._paq = window._paq || []);
  _paq.push(['trackPageView']);
  _paq.push(['enableLinkTracking']);

  (function () {
    var primaryUrl = 'https://matomo.example.com/';

    // Existing Matomo destination
    _paq.push(['setTrackerUrl', primaryUrl + 'matomo.php']);
    _paq.push(['setSiteId', '1']);

    // Additional d8a destination
    _paq.push(['addTracker', 'https://d8a.example.com/matomo.php', '1337']);

    var d = document,
      g = d.createElement('script'),
      s = d.getElementsByTagName('script')[0];
    g.async = true;
    g.src = primaryUrl + 'matomo.js';
    s.parentNode.insertBefore(g, s);
  })();
</script>
```

### Notes

- `addTracker` duplicates tracking requests to both destinations.
- d8a endpoint must be Matomo-compatible (`/matomo.php`).
- The secondary tracker needs its own valid site ID for d8a.

## Option 2: Fully replace Matomo destination with d8a

Use this when you want to keep Matomo tracker API calls in your site, but send data only to d8a.

1. Keep tracking calls (`trackPageView`, `trackEvent`, etc.) unchanged.
2. Point `setTrackerUrl` to your d8a Matomo endpoint.
3. Set `setSiteId` to your d8a site/property ID.
4. Do not add `addTracker`.

```html
<script>
  var _paq = (window._paq = window._paq || []);
  _paq.push(['trackPageView']);
  _paq.push(['enableLinkTracking']);

  (function () {
    // Single destination: d8a
    _paq.push(['setTrackerUrl', 'https://d8a.example.com/matomo.php']);
    _paq.push(['setSiteId', '1337']);

    // Keep loading matomo.js from your current Matomo JS source
    var d = document,
      g = d.createElement('script'),
      s = d.getElementsByTagName('script')[0];
    g.async = true;
    g.src = 'https://cdn.jsdelivr.net/gh/matomo-org/matomo@5.8.0/js/piwik.min.js';
    s.parentNode.insertBefore(g, s);
  })();
</script>
```

### Notes

- This setup sends events only to d8a.
- In this mode, remove this line from the multiple-trackers setup:
  `_paq.push(['addTracker', 'https://d8a.example.com/matomo.php', '1337']);`
