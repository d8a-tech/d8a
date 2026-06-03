---
title: Cookies
sidebar_position: 3
---

Deze pagina documenteert de cookies die door de d8a web tracker worden aangemaakt, hun naamgeving en hun waardeformaten.

## Door de tracker aangemaakte cookies

Tenzij consent expliciet analytics-opslag weigert, maakt en onderhoudt de d8a web tracker:

- `_d8a`: Client ID-cookie (gedeeld over alle d8a-properties binnen dezelfde cookie-prefix-scope).
- `_d8a_<property_id>`: Session context-cookie (één per d8a-property).

Als de `analytics_storage`-consent wordt geweigerd, schrijft de tracker geen analytics-cookies.

In dat geval vermijdt de tracker ook het lezen van bestaande analytics-cookies voor identiteit en gebruikt hij een in-memory identifier voor de client ID die consistent blijft gedurende de levensduur van de single-page application. Zo verandert de client ID niet tussen page views in een single-page app, maar wordt hij gereset als de gebruiker de applicatie herlaadt of verlaat.

De tracker respecteert consent die via een van beide bronnen is ingesteld. Wanneer beide aanwezig zijn, krijgt consent van `gtag('consent', ...)` (GTM/gtag) de voorkeur.

- `d8a('consent', ...)`
- `gtag('consent', ...)` (gelezen uit `window.dataLayer`)

Als je gtag-snippet een aangepaste data layer-naam gebruikt (gtag `l=`), configureer deze dan zodat de tracker consent-updates kan spiegelen:

- **Script tag**: geef `?gl=<queueName>` mee in de `src` van het script (of stel `window.d8aGtagDataLayerName = '<queueName>'` in voordat de tracker wordt geïnstalleerd).
- **npm (module)**: roep `installD8a({ gtagDataLayerName: '<queueName>' })` aan.

## Cookienaamgeving

Cookienamen kunnen worden aangepast met `cookie_prefix` (via `d8a('config', ...)`).

## `_d8a` Client ID-cookie

### Waardeformaat

De cookiewaarde gebruikt een d8a-specifiek voorvoegsel en twee numerieke delen:

`C1.1.<random_31bit_int>.<timestamp_seconds>`

De tracker leest deze cookie om de `cid`-parameter af te leiden als:

`<random_31bit_int>.<timestamp_seconds>`

## `_d8a_<property_id>` Session context-cookie

### Waardeformaat

De session-cookie is een met `$` gescheiden tokenlijst met het voorvoegsel `S1.1.`:

`S1.1.<token>$<token>$...`

Tokens worden opgeslagen als `<key><value>`-paren, waarbij `<key>` één enkel teken is.

### Tokens

- `s`: Session id (timestamp-seconden van het moment waarop de huidige session begon).
- `o`: Session count (verhoogt wanneer er door inactiviteit een nieuwe session wordt aangemaakt).
- `g`: Session engagement-vlag (0/1). Dit wordt verzonden als de `seg`-parameter. De tracker zet hem op `1` nadat de drempel voor geëngageerde tijd is bereikt (`session_engagement_time_sec`, standaard 10).
- `t`: Tijd van laatste activiteit (timestamp-seconden van de laatste hit in de session).
- `j`: Hit-teller per session (verhoogt voor elke hit in dezelfde session).
- `d`: Ondoorzichtige identifier per session (willekeurig, URL-veilig).

### Session-rollover

De tracker maakt een nieuwe session aan wanneer `now - t` `session_timeout_ms` overschrijdt (standaard 30 minuten). Wanneer een nieuwe session wordt aangemaakt:

- `s` wordt ingesteld op de huidige tijd (seconden)
- `o` wordt verhoogd
- `g` wordt gereset naar `0`
- `t` wordt ingesteld op de huidige tijd (seconden)
- `j` wordt gereset naar `0`
- `d` wordt opnieuw gegenereerd

## Cookiedomein auto-selectie {#cookie-domain-auto-selection}

Wanneer `cookie_domain` is ingesteld op `"auto"` (de standaard), selecteert de tracker het breedste geldige cookiedomein door kandidaten van breedst naar smalst te proberen totdat de browser er een accepteert. Dit gtag-compatibele gedrag maakt het mogelijk om cookies waar mogelijk over subdomeinen te delen.

Wanneer de hostnaam van de pagina bijvoorbeeld `docs.d8a.tech` is, probeert de tracker de domeinkandidaten in deze volgorde:

1. `d8a.tech` (breedst — gedeeld over alle `*.d8a.tech`-subdomeinen)
2. `docs.d8a.tech` (smaller — alleen voor dit subdomein)
3. `none` (host-only — geen domeinattribuut, alleen voor de exacte hostnaam)

De tracker gebruikt de eerste kandidaat die de browser accepteert, wat het delen van cookies over subdomeinen mogelijk maakt wanneer het browserbeleid dit toestaat. Dit is vooral handig voor sites met meerdere subdomeinen (bijvoorbeeld `www.example.com`, `docs.example.com`, `app.example.com`) waar je een consistente client ID over alle subdomeinen wilt.

## Cookieconfiguratieopties

Zie voor de volledige lijst met cookiegerelateerde instellingen de [Cookies-sectie in de Configuratiereferentie](/nl/articles/sources/web-tracker/configuration#cookies).
