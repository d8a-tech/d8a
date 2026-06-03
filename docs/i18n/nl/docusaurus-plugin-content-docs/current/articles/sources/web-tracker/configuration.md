---
title: Configuratie
sidebar_position: 2
---

Deze pagina somt alle ondersteunde configuratieopties voor de d8a web tracker op.

Configuratie kan worden opgegeven via:

- `d8a('config', '<property_id>', { ... })`: Configuratie per property.
- `d8a('set', { ... })`: Globale standaardwaarden die op volgende hits worden toegepast (en als fallback worden gebruikt wanneer een property-config geen waarde levert).
- `d8a('set', '<field>', <value>)`: Globale standaardwaarden voor één veld (gelijk aan de objectvorm).

Wanneer dezelfde sleutel op meerdere plaatsen wordt opgegeven, worden waarden als volgt bepaald:

- Event params > config params > set params > browserstandaarden

## Dataverzameling

Deze opties bepalen waar events naartoe worden gestuurd en hoe ze worden gebatcht.

- `server_container_url` (verplicht): Tracking-URL voor een property (de tracker gebruikt dit als het uiteindelijke endpoint).
  - Voorbeeld (cloud): `https://global.t.d8a.tech/80e1d6d0-560d-419f-ac2a-fe9281e93386/d/c`
  - Voorbeeld (on-prem): `https://example.org/d/c`
- `max_batch_size` (optioneel, standaard: `25`): Maximaal aantal in de wachtrij geplaatste events dat in één flush wordt verzonden. Als de wachtrij deze grootte bereikt, flusht de tracker direct (zonder te wachten op `flush_interval_ms`).
- `flush_interval_ms` (optioneel, standaard: `1000`): Tijdgebaseerd flush-interval dat wordt gebruikt wanneer de wachtrij niet vol is. Als er na een flush nog events in behandeling zijn, plant de tracker na deze vertraging een volgende flush.

## Cookies

De tracker beheert twee first-party cookies:

- `_d8a`: Client ID-cookie
- `_d8a_<property_id>`: Session context-cookie

Zie voor de volledige cookiestructuur en voorbeelden [Cookies](/nl/articles/sources/web-tracker/cookies).

Cookie-opties:

- `cookie_domain` (optioneel, standaard: `"auto"`): Strategie voor het cookiedomein. Bij `"auto"` selecteert de tracker automatisch het breedste geldige domein door kandidaten van breedst naar smalst te proberen (zie [Cookiedomein auto-selectie](/nl/articles/sources/web-tracker/cookies#cookie-domain-auto-selection)). Stel in op `"none"` voor host-only cookies (geen domeinattribuut), of geef een expliciete domeinstring op (bijvoorbeeld: `"example.com"`).
- `cookie_path` (optioneel, standaard: `"/"`): Cookiepad.
- `cookie_expires` (optioneel): Levensduur van de cookie in seconden. Indien niet opgegeven, gebruikt de tracker een GA4-achtige standaard van 2 jaar.
- `cookie_prefix` (optioneel, standaard: `""`): Voorvoegsel dat op cookienamen wordt toegepast (handig om identiteiten tussen trackers te isoleren).
- `cookie_update` (optioneel, standaard: `true`): Of de tracker de vervaldatums van cookies bij activiteit vernieuwt. Let op: beveiligingsgerelateerde attribuutupdates (SameSite/Secure/enz.) en het aanmaken van ontbrekende cookies kunnen nog steeds een schrijfactie vereisen.
- `cookie_flags` (optioneel): Ruwe cookie-flags-string (bijvoorbeeld: `SameSite=Strict;Secure`).

## Cross-domain linker

De tracker kan client- en session-context tussen **verschillende domeinen** doorgeven door uitgaande links (en optioneel formulieren) te decoreren met een kortlevende `_dl`-parameter en inkomende `_dl` op het bestemmingsdomein te accepteren.

Zie voor de volledige gids (inclusief technische details) [Cross-domain linking](/nl/articles/sources/web-tracker/cross-domain-linking).

Configuratie wordt ingesteld via:

- `d8a('set', 'linker', { ... })`

Opties:

- `linker.domains` (verplicht): Array van bestemmingsdomeinen. Wanneer een link (of formulier) een hostnaam target die overeenkomt met een van deze strings (substring-match), decoreert de tracker de URL.
- `linker.accept_incoming` (optioneel): Of inkomende `_dl` op de huidige pagina wordt geaccepteerd.
  - Standaard: `true` wanneer `linker.domains` niet leeg is, anders `false`.
- `linker.decorate_forms` (optioneel, standaard: `false`): Indien ingeschakeld, worden ook formulierinzendingen gedecoreerd.
- `linker.url_position` (optioneel, standaard: `'query'`): Waar `_dl` wordt geplaatst (`'query'` of `'fragment'`).

Het cookieschrijfgedrag op het bestemmingsdomein respecteert consent en `cookie_update`:

- Als de `analytics_storage`-consent wordt geweigerd, worden er geen cookies geschreven.
- Als `cookie_update=false`, worden bestaande cookies niet overschreven, maar ontbrekende cookies kunnen nog steeds worden aangemaakt (gtag-achtig).

## Debugging

- `debug_mode` (optioneel): Schakelt debug-logging in en voegt `_dbg=1` en `ep.debug_mode=1` aan trackingverzoeken toe.

## Identiteit en gebruikersvelden

- `user_id` (optioneel): Stelt de gebruikersidentifier in die met trackingverzoeken wordt verzonden (door de tracker in het geheugen opgeslagen).
- `client_id` (optioneel): Overschrijft de client-identifier die met trackingverzoeken wordt verzonden (in plaats van de waarde die uit de client ID-cookie wordt afgeleid).

## Campagne- en page-overrides


- `campaign_id`
- `campaign_source`
- `campaign_medium`
- `campaign_name`
- `campaign_term`
- `campaign_content`
- `page_location`
- `page_title`
- `page_referrer`
- `content_group`
- `language`
- `screen_resolution`
- `ignore_referrer`

## Page view-gedrag

- `send_page_view` (optioneel, standaard: `true`): Bij `true` triggert elke `config`-aanroep automatisch een `page_view`-event voor die property. Stel in op `false` om automatische page views uit te schakelen.

## Enhanced measurement {#enhanced-measurement}

- `site_search_enabled` (optioneel, standaard: `true`): Schakelt het automatisch vastleggen van site search in.
- `site_search_query_params` (optioneel, standaard: `"q,s,search,query,keyword"`): Sleutels van zoekqueryparameters (CSV-string of string-array).
- `outbound_clicks_enabled` (optioneel, standaard: `true`): Schakelt het automatisch vastleggen van outbound clicks in.
- `outbound_exclude_domains` (optioneel): Domeinen die van outbound-tracking worden uitgesloten (CSV-string of string-array). Standaard de hostnaam van de huidige site.
- `file_downloads_enabled` (optioneel, standaard: `true`): Schakelt het automatisch vastleggen van file downloads in.
- `file_download_extensions` (optioneel): Bestandsextensies die als downloads worden beschouwd (CSV-string of string-array). Standaard: `pdf,doc,docx,xls,xlsx,ppt,pptx,csv,txt,rtf,zip,rar,7z,dmg,exe,apk`.

## Engagement

Engagement-gerelateerde opties:

- `session_engagement_time_sec` (optioneel, standaard: `10`): Minimale geëngageerde tijd (in seconden) die nodig is om `seg=1` voor de session in te schakelen.
- `session_timeout_ms` (optioneel, standaard: `1800000`): Session-timeoutvenster dat voor de Session context-cookie wordt gebruikt. Let op: dit beïnvloedt alleen de client-side session-status van de web tracker (bijvoorbeeld de `session_id` die hij verzendt). D8a berekent sessions ook op de backend.
