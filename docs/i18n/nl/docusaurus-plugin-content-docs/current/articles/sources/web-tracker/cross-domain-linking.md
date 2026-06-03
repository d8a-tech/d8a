---
title: Cross-domain linking
sidebar_position: 4
---

Gebruik cross-domain linking wanneer een user journey zich uitstrekt over **meerdere domeinen die geen cookie-basisdomein delen** (bijvoorbeeld `store.example` → `example-checkout.com`). De d8a web tracker kan client- en session-continuïteit behouden door uitgaande URL's te decoreren met een kortlevende `_dl`-parameter.

## Installatie

Cross-domain linking vereist dat de web tracker op **zowel** het brondomein als het bestemmingsdomein is geïnstalleerd.

Volg eerst de [Web tracker-snelstart](/nl/articles/sources/web-tracker/#quick-start) op elk domein en configureer daarna de linker.

## Configuratie

Configureer de linker met `set('linker', ...)`:

<!-- prettier-ignore -->
```javascript
// Decorate links that point to these domains.
d8a('set', 'linker', {
  domains: ['example-checkout.com'],
  // Optional:
  // decorate_forms: true,
  // url_position: 'query', // default
  // accept_incoming: true, // default when domains is non-empty
});
```

Opties:

- **`domains`** (verplicht): Bestemmingsdomeinen die moeten worden gedecoreerd wanneer een link of formulier naar een overeenkomende hostnaam verzendt.
- **`decorate_forms`** (optioneel, standaard: `false`): Indien ingeschakeld, decoreert de tracker ook formulierinzendingen.
- **`url_position`** (optioneel, standaard: `'query'`): Waar `_dl` wordt geplaatst (`'query'` of `'fragment'`).
  - Voorbeeld (`'query'`): `https://example-checkout.com/checkout?_dl=<payload>`
  - Voorbeeld (`'fragment'`): `https://example-checkout.com/checkout#_dl=<payload>`
- **`accept_incoming`** (optioneel): Of de bestemmingssite inkomende `_dl` moet accepteren.
  - Standaard: ingeschakeld wanneer `domains` niet leeg is.
  - Als je alleen `_dl` wilt **accepteren** (maar geen uitgaande links wilt decoreren), stel dan `domains: []` en `accept_incoming: true` in.

Het gedrag bij het overschrijven van cookies wordt bepaald door `cookie_update` (zie [Configuratie](/nl/articles/sources/web-tracker/configuration#cookies)):

- Als `cookie_update=false`, worden bestaande cookies **niet overschreven**, maar ontbrekende cookies kunnen nog steeds worden aangemaakt.

## Hoe het werkt (technisch overzicht)

Op hoofdlijnen:

- **Uitgaand**: Wanneer de gebruiker op een uitgaande `<a>` klikt (en optioneel een formulier verzendt), controleert de tracker of de bestemmingshostnaam overeenkomt met `linker.domains`. Zo ja, dan voegt hij een `_dl`-parameter toe.
  - Als er meerdere tracker-instanties (`d8a1`, `d8a2`, …) op dezelfde pagina draaien, delen ze één linker en produceren ze toch **één enkele** `_dl`-waarde. De payload bevat cookies voor **alle geconfigureerde properties / cookie-prefixes** over die instanties.
  - Decoratie wordt lui uitgevoerd bij gebruikersinteractie door te luisteren naar `document`-events:
    - `mousedown` en `keyup` voor links, en
    - `submit` voor formulieren.
      Daarnaast worden programmatische formulierinzendingen afgehandeld door `HTMLFormElement.prototype.submit` te patchen.
- **Payload**: `_dl` draagt een compacte, URL-veilige codering van de **geserialiseerde d8a-cookies** (client- + session-cookies). De payload is:
  - **kortlevend** (ongeveer 2 minuten), en
  - beschermd door een **niet-geheime fingerprint/hash** (UA + tijdzone + taal + tijdvenster + payload) om accidentele corruptie te beperken.
- **Inkomend**: Op het bestemmingsdomein leest de tracker `_dl` uit de URL, valideert deze en **verwijdert vervolgens `_dl` uit de URL** (met `history.replaceState`) om de URL schoon te houden. Het verwijderen gebeurt ongeacht of de validatie slaagt.
- **Identiteit toepassen**:
  - Als de `analytics_storage`-consent wordt geweigerd, schrijft de tracker geen cookies en bewaart hij de inkomende client id in het geheugen voor het verzenden van hits.
  - Als consent cookies toestaat, schrijft de tracker de inkomende cookies met de huidige cookie-instellingen (domein/pad/prefix/enz.) en respecteert hij `cookie_update`.
  - Als er nog niet genoeg configuratie beschikbaar is om de cookies in `_dl` te herkennen (bijvoorbeeld voordat de cookie-prefixes van properties bekend zijn), bewaart de tracker de inkomende payload en past hij cookies **incrementeel** toe naarmate er configuratie beschikbaar komt.
  - Wanneer een inkomende **session**-cookie wordt toegepast, seedt de tracker ook de in-memory session-status, zodat een onmiddellijke `page_view` niet per ongeluk een nieuwe session start.
