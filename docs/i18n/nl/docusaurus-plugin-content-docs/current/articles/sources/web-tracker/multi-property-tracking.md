---
title: Tracking voor meerdere properties
sidebar_position: 5
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Deze gids beschrijft hoe je **meerdere d8a-properties** vanaf dezelfde site trackt met de d8a web tracker, met gtag-achtige semantiek (meerdere `config`-aanroepen en optionele routing per event).

In deze gids:

- `<property_id_1>`, `<property_id_2>`, enz. zijn d8a-property-ID's (UUID-formaat).

## Use case 1: Eén trackingcode, meerdere properties (fan-out)

### Overzicht van de use case

Gebruik dit wanneer je één `d8a()`-API en één queue wilt, en je elk event naar meerdere properties wilt sturen.

### Codevoorbeeld

<div className="d8a-tabs-border">
<Tabs>
<TabItem value="script-tag" label="Script tag" default>

<!-- prettier-ignore -->
```html
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js"></script>
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
import { installD8a } from '@d8a-tech/wt';

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
</div>

### Details

- **Configuratie**: roep `d8a('config', ...)` één keer per property aan. Daarna worden events standaard naar alle geconfigureerde properties verspreid (fan-out).
- **Cookies (standaardgedrag)**:
  - **Eén gedeelde** Client ID-cookie (`_d8a`).
  - **Eén Session context-cookie per property** (`_d8a_<property_id_1>` en `_d8a_<property_id_2>`), met **identieke waarden** wanneer beide properties dezelfde events ontvangen.

#### Identiteiten isoleren

Door voor elke property een andere `cookie_prefix` in te stellen, isoleer je de data van de Client ID-cookie volledig tussen properties.

::::note
De Session context-cookie (`_d8a_<property_id>`) is altijd apart voor elke property, maar de waarden zullen identiek zijn over properties.
::::

#### Aantal cookies (bij het tracken van 2 properties)

- Als je `cookie_prefix` **niet** instelt:
  - **In totaal 3 cookies**: `_d8a` + `_d8a_<property_id_1>` + `_d8a_<property_id_2>`
- Als je voor beide properties **dezelfde** `cookie_prefix` instelt:
  - **In totaal 3 cookies**: `<prefix>_d8a` + `<prefix>_d8a_<property_id_1>` + `<prefix>_d8a_<property_id_2>`
- Als je **verschillende** `cookie_prefix`-waarden per property instelt:
  - **In totaal 4 cookies**: `property1_d8a` + `property1_d8a_<property_id_1>` + `property2_d8a` + `property2_d8a_<property_id_2>`

## Use case 2: Twee onafhankelijke trackers op dezelfde pagina (twee globals en twee queues)

### Overzicht van de use case

Gebruik dit wanneer je strikte scheiding tussen trackers wilt: elke instantie krijgt volledig aparte queues, globale functienamen en levenscyclus - **en zowel de Client ID-cookie (`_d8a`) als de Session context-cookie (`_d8a_<property_id>`) worden volledig geïsoleerd tussen trackers**.

### Codevoorbeeld

<div className="d8a-tabs-border">
<Tabs>
<TabItem value="script-tag-2" label="Script tag" default>

<!-- prettier-ignore -->
```html
<!-- Instance 1 (defaults: global=d8a, queue=d8aLayer) -->
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js"></script>

<!-- Instance 2 (global=d8a2, queue=d8aLayer2) -->
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js?l=d8aLayer2&g=d8a2"></script>
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
import { installD8a } from '@d8a-tech/wt';

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
</div>

## Use case 3: Een specifieke property per event targeten (`send_to`)

### Overzicht van de use case

Gebruik dit wanneer je in het algemeen meerdere properties configureert, maar sommige events alleen naar één property (of een subset) wilt sturen.

### Codevoorbeeld

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

- Als `send_to` in de event-params aanwezig is, stuurt `d8a` dat event alleen naar de opgegeven property (of subset).
- `send_to` kan ofwel een enkele property-ID-string zijn, ofwel een array van property-ID's.
- Elke property waar je events naartoe stuurt, moet correct geconfigureerd zijn (stel met name `server_container_url` in voor elke property).
