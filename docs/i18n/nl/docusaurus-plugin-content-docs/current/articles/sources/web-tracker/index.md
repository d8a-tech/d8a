---
title: Web tracker
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

De d8a web tracker biedt een GA4 gtag-achtige API die GA4 gtag-compatibele verzoeken rechtstreeks naar een d8a-collector stuurt. Gebruik hem wanneer je GA4-achtige tracking wilt zonder afhankelijk te zijn van door Google gehoste tags.

:::caution Beta
De web tracker is momenteel in **beta** - meld gerust eventuele problemen via [GitHub Issues](https://github.com/d8a-tech/d8a/issues).
:::

## Snelstart {#quick-start}

<div className="d8a-tabs-border">
<Tabs>
<TabItem value="script-tag" label="Script tag" default>
Plak deze code zo hoog mogelijk in de `<head>` van de pagina:

<!-- prettier-ignore -->
```html
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js"></script>
<script>
  window.d8aLayer = window.d8aLayer || [];
  window.d8a = window.d8a || function(){d8aLayer.push(arguments);};

  d8a('js', new Date());
  d8a('config', '<property_id>', {
    server_container_url: 'https://global.t.d8a.tech/<property_id>/d/c'
  });
</script>
```

</TabItem>
<TabItem value="npm-module" label="npm (module)">

Installeer eerst het pakket:

```bash
npm install @d8a-tech/wt
```

Gebruik vervolgens de volgende code om te initialiseren:

<!-- prettier-ignore -->
```javascript
import { installD8a } from '@d8a-tech/wt';

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
</div>

::::note
Standaard is `send_page_view` ingeschakeld, dus de tracker verstuurt een `page_view`. Enhanced measurement is ook standaard ingeschakeld (zie [Enhanced measurement](#enhanced-measurement)). De `d8a()`-functie biedt ook gemaksmethoden (`d8a.config`, `d8a.event`, `d8a.set`, `d8a.consent`) die zich hetzelfde gedragen als de string-commandovorm.
::::

## Configuratie

- `server_container_url`: Verplicht. Tracking-URL voor deze property (de tracker gebruikt dit als het uiteindelijke endpoint).
  - Voorbeeld (d8a Cloud): `https://global.t.d8a.tech/80e1d6d0-560d-419f-ac2a-fe9281e93386/d/c`
  - Voorbeeld (self-hosted): `https://example.org/d/c`
- Zie voor alle beschikbare opties [Configuratie](/nl/articles/sources/web-tracker/configuration).

::::note
De web tracker werkt ook wanneer het GA4-trackingprotocol is ingeschakeld. Dit is handig als je oorspronkelijk GA4-verkeer naar d8a hebt omgeleid en nu data wilt blijven sturen naar dezelfde property met de web tracker. In dat geval moet je het endpoint wijzigen van `/d/c` naar `/g/collect`.
::::

## Events

De tracker ondersteunt het verzenden van events met `d8a('event', '<event_name>', { ... })`. Hier zijn enkele van de meest gebruikte events:

### Page view

Automatisch verzonden wanneer een property is geconfigureerd (tenzij `send_page_view: false`). Je kunt het ook handmatig verzenden:

<!-- prettier-ignore -->
```javascript
d8a('event', 'page_view', {
  page_title: 'Homepage',
  page_location: 'https://example.com/',
});
```

### Add to cart

Track wanneer gebruikers items aan hun winkelwagen toevoegen:

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

Track voltooide aankopen:

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

Je kunt elke custom event name verzenden:

<!-- prettier-ignore -->
```javascript
d8a('event', 'newsletter_signup', {
  newsletter_type: 'weekly',
  source: 'homepage',
});
```

Zie voor een volledige lijst met aanbevolen events en hun parameters de [documentatie over aanbevolen GA4-events](https://developers.google.com/analytics/devguides/collection/ga4/reference/events).

## Enhanced measurement {#enhanced-measurement}

De tracker kan automatisch GA4-achtige events vastleggen (standaard ingeschakeld):

- **Site search**: Vuurt `view_search_results` af wanneer de URL-querystring een bekende zoekparameter bevat.
  - Standaardsleutels: `q,s,search,query,keyword`
- **Outbound clicks**: Vuurt `click` af met `link_url`, `link_domain`, `outbound=1`.
- **File downloads**: Vuurt `file_download` af met `link_url`, `file_name`, `file_extension`.

Je kunt deze configureren (of uitschakelen) via `d8a('config', ...)` of `d8a('set', ...)`. Zie voor alle beschikbare configuratieopties [Enhanced measurement-instellingen](/nl/articles/sources/web-tracker/configuration#enhanced-measurement).

Voorbeeld (alle auto-events uitschakelen):

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
Enhanced measurement is bewust lichtgewicht:

- Site search wordt hoogstens eenmaal per page load getriggerd (geen SPA-tracking daarbuiten).
- Door clicks aangestuurde enhanced measurement-events worden direct geflusht om drop-off bij navigatie te beperken.
- De tracker ondersteunt **geen** automatische SPA-page views, scroll-tracking, formulierinteracties of video-engagement. De standaard `gtag.js`-implementaties hiervoor zijn vaak te generiek of onbetrouwbaar voor productiebehoeften. We raden aan deze te implementeren via een Tag Manager (zoals GTM) of custom event listeners voor nauwkeurige controle.
  :::::

## Cookies

De tracker beheert twee first-party cookies (tenzij consent expliciet analytics-opslag weigert):

- `_d8a`: Client ID-cookie (waardeformaat: `C1.1.<a>.<b>`).
- `_d8a_<property_id>`: Session context-cookie (waardeformaat: `S1.1...`).

Zie voor details [Cookies](/nl/articles/sources/web-tracker/cookies).

## Engagement

De tracker zendt `user_engagement`-events uit om de backend te laten weten of de gebruiker daadwerkelijk geëngageerd was.

Wanneer de `analytics_storage`-consent verandert nadat ten minste één property is geconfigureerd, zendt de tracker een lichtgewicht `user_engagement`-ping uit, gemarkeerd met `ep.consent_update=1`.

Elk event bevat `_et` (engagement time in milliseconden): tijd die alleen wordt opgebouwd terwijl de pagina actief, zichtbaar en in focus is.

Wanneer de opgebouwde `_et` voor de huidige session de engagementdrempel bereikt (standaard: 10 seconden, configureerbaar via `session_engagement_time_sec`), werkt de tracker de Session context-cookie bij om de session als geëngageerd te markeren. Daarna bevatten volgende events van die session `seg=1`.

## Gerelateerde documentatie

- [GA4 gtag-protocolreferentie](/nl/articles/tracking-protocols/ga4-gtag)
