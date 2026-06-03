---
title: Overstappen vanaf gtag.js
sidebar_position: 6
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Deze gids beschrijft hoe je een bestaande GA4-opstelling die `gtag()` en `dataLayer` gebruikt, migreert naar de d8a web tracker, terwijl je **je bestaande `gtag()`-aanroepen behoudt**.

## Voordat je begint

Deze aanpak werkt het beste wanneer:

- Je site **`gtag('event', ...)`** (en optioneel `gtag('set', ...)`, `gtag('consent', ...)`) gebruikt om events uit te zenden.
- Je Google's script-tag-installatie voor GA4 kunt verwijderen (zodat er geen tweede `gtag()`-implementatie op de pagina staat).

:::warning
Dit is **geen** drop-in-vervanging voor GTM-achtige pushes zoals `dataLayer.push({ event: '...' })`. De d8a web tracker consumeert **array-achtige** items (snippet-stijl `arguments`), geen gewone objecten (bijvoorbeeld `dataLayer.push(['event', 'purchase', { value: 123 }])`).
:::

## Stap 1: Verwijder Google's gtag-installatie

Verwijder het GA4-laadsnippet van je pagina. Het ziet er doorgaans zo uit:

- Het script dat Google's tag laadt (voorbeeld): `https://www.googletagmanager.com/gtag/js?...`
- Het inline initialisatiesnippet dat `window.dataLayer` en `function gtag(){dataLayer.push(arguments)}` definieert
- Eventuele `gtag('config', 'G-XXXX', ...)`-aanroepen die Google Analytics configureren

Na deze stap zou je site geen events meer naar Google Analytics moeten sturen.

## Stap 2: Installeer d8a als `gtag()` met `dataLayer`

Installeer d8a met:

- `dataLayerName: 'dataLayer'` (om aan te sluiten op de GA4-conventie)
- `globalName: 'gtag'` (zodat je bestaande event-aanroepen blijven werken)

<div className="d8a-tabs-border">
<Tabs>
<TabItem value="script-tag" label="Script tag" default>

<!-- prettier-ignore -->
```html
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js?l=dataLayer&g=gtag"></script>
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
import { installD8a } from '@d8a-tech/wt';

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
</div>

## Stap 3: Behoud je bestaande `gtag('event', ...)`-aanroepen

Eenmaal geïnstalleerd zouden je bestaande aanroepen moeten blijven werken (nu naar d8a verzendend), bijvoorbeeld:

<!-- prettier-ignore -->
```javascript
gtag('event', 'purchase', {
  currency: 'USD',
  value: 149.97
});
```

## Opmerkingen

- **`server_container_url` is verplicht**: zonder dit kan de tracker de uiteindelijke endpoint-URL voor het verzenden niet samenstellen.
- **Property-ID's**: het eerste argument van `gtag('config', ...)` moet je **d8a**-`<property_id>` zijn (niet een GA4 `G-XXXX`-measurement-ID).
- **Consent**: `gtag('consent', ...)` wordt ondersteund.
