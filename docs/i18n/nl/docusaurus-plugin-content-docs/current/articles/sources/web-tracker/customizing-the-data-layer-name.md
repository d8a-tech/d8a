---
title: De data layer-naam aanpassen
sidebar_position: 4
---

Standaard gebruikt de d8a web tracker een queue genaamd `d8aLayer` (niet `dataLayer`). Dit voorkomt conflicten met Google Tag Manager (GTM) en `gtag.js`, die doorgaans `dataLayer` gebruiken.

Je kunt de queue-naam aanpassen naar elke string, zolang:

- Je snippet naar die queue pusht.
- de d8a web tracker is geconfigureerd om dezelfde queue te consumeren.

## Optie 1: Script tag met `?l=` (gtag-stijl)

Gebruik dit wanneer je de d8a web tracker via een script tag laadt en de configuratie lokaal bij de script-URL wilt houden.

<!-- prettier-ignore -->
```html
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js?l=myQueue"></script>

<script>
  window.myQueue = window.myQueue || [];
  window.d8a = window.d8a || function(){myQueue.push(arguments);};

  d8a('js', new Date());
  d8a('config', '<property_id>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
  });
</script>
```

## Optie 2: Stel `window.d8aDataLayerName` in

Gebruik dit wanneer je de d8a web tracker via een script tag laadt, maar de queue-naam liever in code instelt.

<!-- prettier-ignore -->
```html
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js"></script>

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

## Optie 3: Geef de data layer-naam op bij het aanroepen van de install-methode

Gebruik dit wanneer je het pakket via npm (module) installeert en `dataLayerName` aan `installD8a()` doorgeeft, zodat de tracker dezelfde queue consumeert die je gebruikt om aanroepen te bufferen.

Installeer het pakket:

```bash
npm install @d8a-tech/wt
```

<!-- prettier-ignore -->
```javascript
import { installD8a } from '@d8a-tech/wt';

installD8a({ dataLayerName: 'myQueue' });

const d8a = window.d8a;
if (!d8a) throw new Error('d8a is not installed');

d8a('js', new Date());
d8a('config', '<property_id>', {
  server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
});
```
