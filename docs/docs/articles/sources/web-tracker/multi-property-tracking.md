---
title: Multi-property tracking
sidebar_position: 5
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide describes how to track **multiple d8a properties** from the same site with the d8a web tracker, using gtag-style semantics (multiple `config` calls and optional per-event routing).

Throughout this guide:

- `<property_id_1>`, `<property_id_2>`, etc. are d8a property IDs (UUID format).

## Use case 1: One tracking code, multiple properties (fan-out)

### Use case overview

Use this when you want one `d8a()` API and one queue, and you want every event to be sent to multiple properties.

### Code example

<Tabs>
<TabItem value="script-tag" label="Script tag" default>

<!-- prettier-ignore -->
```html
<script async src="https://global.t.d8a.tech/js"></script>
<script>
  window.d8aLayer = window.d8aLayer || [];
  window.d8a = window.d8a || function(){d8aLayer.push(arguments);};

  d8a('js', new Date());

  // Property 1
  d8a('config', '<property_id_1>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id_1>/d/c'
  });

  // Property 2
  d8a('config', '<property_id_2>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id_2>/d/c'
  });

</script>
```

</TabItem>
<TabItem value="npm-module" label="npm (module)">

<!-- prettier-ignore -->
```javascript
import { installD8a } from '@d8a-tech/web-tracker';

installD8a();

const d8a = window.d8a;
if (!d8a) throw new Error('d8a is not installed');

d8a('js', new Date());

// Property 1
d8a('config', '<property_id_1>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id_1>/d/c'
});

// Property 2
d8a('config', '<property_id_2>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id_2>/d/c'
});
```

</TabItem>
</Tabs>

### Details

- **Configuration**: call `d8a('config', ...)` once per property. After that, events fan-out to all configured properties by default.
- **Cookies (default behavior)**:
  - **One shared** Client ID cookie (`_d8a`).
  - **One Session context cookie per property** (`_d8a_<property_id_1>` and `_d8a_<property_id_2>`), with **identical values** when both properties receive the same events.

#### Isolating identities

Setting a different `cookie_prefix` for each property will fully isolate the client ID cookie data between properties.

::::note
The session context cookie (`_d8a_<property_id>`) is always separate for each property, but the values will be identical across properties.
::::

#### Cookie counts (when tracking 2 properties)

- If you do **not** set `cookie_prefix`:
  - **3 cookies total**: `_d8a` + `_d8a_<property_id_1>` + `_d8a_<property_id_2>`
- If you set the **same** `cookie_prefix` for both properties:
  - **3 cookies total**: `<prefix>_d8a` + `<prefix>_d8a_<property_id_1>` + `<prefix>_d8a_<property_id_2>`
- If you set **different** `cookie_prefix` values per property:
  - **4 cookies total**: `property1_d8a` + `property1_d8a_<property_id_1>` + `property2_d8a` + `property2_d8a_<property_id_2>`

## Use case 2: Two independent trackers on the same page (two globals and two queues)

### Use case overview

Use this when you want strict separation between trackers: each instance will have completely separate queues, global function names, and lifecycle - **and both the Client ID cookie (`_d8a`) and Session context cookie (`_d8a_<property_id>`) will be totally isolated between trackers**.

### Code example

<Tabs>
<TabItem value="script-tag-2" label="Script tag" default>

<!-- prettier-ignore -->
```html
<!-- Instance 1 (defaults: global=d8a, queue=d8aLayer) -->
<script async src="https://global.t.d8a.tech/js"></script>

<!-- Instance 2 (global=d8a2, queue=d8aLayer2) -->
<script async src="https://global.t.d8a.tech/js?l=d8aLayer2&g=d8a2"></script>
<script>
  // Instance 1 (default)
  window.d8aLayer = window.d8aLayer || [];
  window.d8a = window.d8a || function(){d8aLayer.push(arguments);};

  d8a('js', new Date());
  d8a('config', '<property_id_1>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id_1>/d/c',
    cookie_prefix: 'prop1'
  });

  // Instance 2 (separate queue + separate global)
  window.d8aLayer2 = window.d8aLayer2 || [];
  window.d8a2 = window.d8a2 || function(){d8aLayer2.push(arguments);};

  d8a2('js', new Date());
  d8a2('config', '<property_id_2>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id_2>/d/c',
    cookie_prefix: 'prop2'
  });
</script>
```

</TabItem>
<TabItem value="npm-module-2" label="npm (module)">

<!-- prettier-ignore -->
```javascript
import { installD8a } from '@d8a-tech/web-tracker';

// Instance 1 (defaults: global=d8a, queue=d8aLayer)
installD8a();

// Instance 2 (separate queue + separate global)
installD8a({ globalName: 'd8a2', dataLayerName: 'd8aLayer2' });

const d8a = window.d8a;
if (!d8a) throw new Error('d8a is not installed');
const d8a2 = window.d8a2;
if (!d8a2) throw new Error('d8a2 is not installed');

d8a('js', new Date());
d8a('config', '<property_id_1>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id_1>/d/c',
  cookie_prefix: 'prop1'
});

d8a2('js', new Date());
d8a2('config', '<property_id_2>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id_2>/d/c',
  cookie_prefix: 'prop2'
});
```

</TabItem>
</Tabs>

## Use case 3: Target a specific property per event (`send_to`)

### Use case overview

Use this when you generally configure multiple properties, but you want some events to be sent only to one property (or a subset).

### Code example

```js
// Send only to one property
d8a("event", "special_event", {
  send_to: "<property_id_1>",
  parameter: "value",
});

// Send only to a subset of properties
d8a("event", "special_event", {
  send_to: ["<property_id_1>", "<property_id_2>"],
  parameter: "value",
});
```

### Details

- If `send_to` is present in the event params, `d8a` sends that event only to the specified property (or subset).
- `send_to` can be either a single property ID string or an array of property IDs.
- Each property you send events to must be configured correctly (in particular, set `server_container_url` for every property).
