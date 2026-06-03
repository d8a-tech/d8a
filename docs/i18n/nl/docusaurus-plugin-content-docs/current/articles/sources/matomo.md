---
title: Matomo
sidebar_position: 2
---

Deze gids laat twee manieren zien om de Matomo-tracker met d8a te gebruiken:

- tracking dupliceren naar zowel Matomo als d8a (meerdere trackers),
- of de Matomo-bestemming volledig vervangen door d8a met het Matomo-protocol.

Bronconcept: [Matomo - Multiple Matomo trackers](https://developer.matomo.org/guides/tracking-javascript-guide#multiple-piwik-trackers).

:::warning
Ingestie via het Matomo-protocol wordt momenteel alleen ondersteund in d8a OSS. Cloud-ondersteuning komt binnenkort.
:::

## Optie 1: Meerdere trackers (behoud Matomo en verzend naar d8a)

Gebruik deze opstelling wanneer:

- je site al de Matomo JavaScript-tracker draait,
- je de huidige Matomo-rapportage wilt behouden,
- en je dezelfde events in d8a wilt hebben.

## Setup

1. Houd je bestaande Matomo-trackerconfiguratie (`setTrackerUrl`, `setSiteId`) ongewijzigd.
2. Voeg één `addTracker`-aanroep toe die naar je d8a Matomo-compatibele endpoint wijst.
3. Houd de normale tracking-aanroepen (`trackPageView`, `trackEvent`, enz.) ongewijzigd.

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

### Opmerkingen

- `addTracker` dupliceert trackingverzoeken naar beide bestemmingen.
- Het d8a-endpoint moet Matomo-compatibel zijn (`/matomo.php`).
- De secundaire tracker heeft zijn eigen geldige site-ID voor d8a nodig.

## Optie 2: De Matomo-bestemming volledig vervangen door d8a

Gebruik dit wanneer je de aanroepen van de Matomo-tracker-API in je site wilt behouden, maar data alleen naar d8a wilt sturen.

1. Houd de tracking-aanroepen (`trackPageView`, `trackEvent`, enz.) ongewijzigd.
2. Wijs `setTrackerUrl` naar je d8a Matomo-endpoint.
3. Stel `setSiteId` in op je d8a-site/property-ID.
4. Voeg geen `addTracker` toe.

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

### Opmerkingen

- Deze opstelling stuurt events alleen naar d8a.
- Verwijder in deze modus deze regel uit de opstelling met meerdere trackers:
  `_paq.push(['addTracker', 'https://d8a.example.com/matomo.php', '1337']);`
