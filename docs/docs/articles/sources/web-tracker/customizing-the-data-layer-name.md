---
title: Customizing the data layer name
sidebar_position: 4
---

By default, the d8a web tracker uses a queue named `d8aLayer` (not `dataLayer`). This avoids conflicts with Google Tag Manager (GTM) and `gtag.js`, which commonly use `dataLayer`.

You can customize the queue name to any string as long as:

- Your snippet pushes to that queue.
- the d8a web tracker is configured to consume the same queue.

## Option 1: Script tag with `?l=` (gtag-style)

Use this when you load the d8a web tracker via a script tag and want to keep the configuration local to the script URL.

<!-- prettier-ignore -->
```html
<script async src="https://global.t.d8a.tech/d/wt.min.js?l=myQueue"></script>

<script>
  window.myQueue = window.myQueue || [];
  window.d8a = window.d8a || function(){myQueue.push(arguments);};

  d8a('js', new Date());
  d8a('config', '<property_id>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
  });
</script>
```

## Option 2: Set `window.d8aDataLayerName`

Use this when you load the d8a web tracker via a script tag but prefer setting the queue name in code.

<!-- prettier-ignore -->
```html
<script async src="https://global.t.d8a.tech/d/wt.min.js"></script>

<script>
  window.d8aDataLayerName = 'myQueue';

  window.myQueue = window.myQueue || [];
  window.d8a = window.d8a || function(){myQueue.push(arguments);};

  d8a('js', new Date());
  d8a('config', '<property_id>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
  });
</script>
```

## Option 3: Specify data layer name when calling the install method

Use this when you install the package via npm (module) and pass `dataLayerName` to `installD8a()` so the tracker consumes the same queue you use for buffering calls.

Install the package:

```bash
npm install @d8a-tech/web-tracker
```

<!-- prettier-ignore -->
```javascript
import { installD8a } from '@d8a-tech/web-tracker';

installD8a({ dataLayerName: 'myQueue' });

const d8a = window.d8a;
if (!d8a) throw new Error('d8a is not installed');

d8a('js', new Date());
d8a('config', '<property_id>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
});
```
