# GA4 gtag

Deze pagina is een referentie voor het GA4 gtag-protocol `/g/collect`.

## Methode

`POST`

## URL

Veelvoorkomende endpoints zijn onder andere:

- `https://www.google-analytics.com/g/collect`
- `https://region1.google-analytics.com/g/collect`


## Queryparameters

### Request-parameters

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `v` | string | Protocolversie | Ja |
| `tid` | string | Measurement ID (bijv. G-XXXXXXXXXX) | Ja |
| `gtm` | string | GTM Has Info - geeft informatie over Google Tag Manager aan | Nee |
| `_p` | int64 | Random Page Load Hash - UTC Unix-timestamp van de page load, in milliseconden | Ja |
| `sr` | string | Schermresolutie - bijv. "1920x1080" | Nee |
| `ul` | string | Gebruikerstaal - bijv. "en-us" | Nee |
| `dh` | string | Document Hostname - hostnaam van de pagina | Nee |
| `cid` | string | Client ID - unieke identifier voor de client | Ja |
| `_s` | int64 | Hit Counter - geeft aan dat dit de n-de hit/event is die door de huidige gtag-runtime-instantie is verzonden (reset bij page load) | Nee |
| `richsstsse` | string | richsstsse-parameter | Nee |

### Event-parameters

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `en` | string | Event Name - naam van het getrackte event | Ja |
| `_et` | int64 | Engagement Time - tijd dat de gebruiker met de pagina interacteerde, in milliseconden | Nee |
| `ep.*` | string | Event Parameter (String) - custom event-parameter | Nee |
| `epn.*` | number | Event Parameter (Number) - numerieke custom event-parameter | Nee |
| `_c` | bool | is Conversion - geeft aan of het event een conversie is | Nee |
| `_ee` | bool | External Event - geeft aan of het event extern is | Nee |

### Gedeelde parameters

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `dl` | string | Document Location - URL van de pagina | Nee |
| `dt` | string | Document Title - titel van de pagina | Nee |
| `dr` | string | Document Referrer - referrer-URL | Nee |
| `_eu` | string | Event Usage | Nee |
| `_edid` | string | Event Debug ID | Nee |
| `_dbg` | bool | is Debug - geeft de debug-modus aan | Nee |
| `ir` | bool | Ignore Referrer - indien aanwezig en true wordt de referrer genegeerd. [GA4-documentatie](https://support.google.com/analytics/answer/10327750?hl=en) | Nee |
| `tt` | string | Traffic Type | Nee |
| `gcs` | string | Codeert de huidige Google consent-status in het formaat `G<FunctionalStorageStatus><AdStorageStatus><AnalyticsStorageStatus>` | Nee |
| `gcu` | string | Google Consent Update. Deze parameter wordt alleen verzonden bij een wijziging in ad_storage, en niet wanneer analytics_storage verandert | Nee |
| `gcut` | string | Google Consent Update Type. Deze parameter wordt alleen verzonden bij een wijziging in ad_storage, en niet wanneer analytics_storage verandert | Nee |
| `gcd` | string | Google Consent Default | Nee |
| `_glv` | bool | is Google Linker Valid | Nee |

### E-commerce-parameters

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `cu` | string | Currency Code - bijv. "USD", "EUR" | Nee |
| `ep.affiliation` | string | Affiliation - winkel of affiliatie | Nee |
| `epn.value` | number | Transaction Revenue - totale transactiewaarde | Nee |
| `epn.tax` | number | Transaction Tax | Nee |
| `epn.shipping` | number | Transaction Shipping | Nee |
| `pr[0-9]{1,200}` | object | Item - product-/item-data (ondersteunt maximaal 200 items) | Nee |
| `pi` | string | Promotion ID | Nee |
| `pn` | string | Promotion Name | Nee |
| `cn` | string | Creative Name | Nee |
| `cs` | string | Creative Slot | Nee |
| `li` | string | Location ID | Nee |

### Parameters voor campagne-attributie

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `cm` | string | Campaign Medium - bijv. "cpc", "email" | Nee |
| `cs` | string | Campaign Source - bijv. "google", "newsletter" | Nee |
| `cn` | string | Campaign Name | Nee |
| `cc` | string | Campaign Content | Nee |
| `ct` | string | Campaign Term | Nee |
| `ccf` | string | Campaign Creative Format | Nee |
| `cmt` | string | Campaign Marketing Tactic | Nee |
| `_rnd` | string | GCLID Deduper | Nee |

### User- & session-parameters

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `uid` | string | User ID - optionele gebruikersidentifier | Nee |
| `_fid` | string | Firebase ID | Nee |
| `sid` | string | Session ID - unieke session, toegekend door de client wanneer de session start, daarna doorgegeven aan elk event | Ja |
| `sct` | int64 | Session Count - aantal sessions voor de gebruiker | Nee |
| `seg` | int64 | Session Engagement - engagementniveau van de session | Nee |
| `up.*` | string | User Property(ies) - custom user properties | Nee |
| `upn.*` | number | User Property Value - numerieke user property | Nee |
| `_fv` | bool | First Visit - geeft het eerste bezoek aan | Nee |
| `_ss` | bool | Session Start - geeft aan dat dit event de session start. Indien aanwezig, altijd waarde `1` | Nee |
| `_fplc` | string | First Party Linker Cookie | Nee |
| `nsi` | string | New Session ID | Nee |
| `gdid` | string | Google Developer ID | Nee |
| `_uc` | string | User Country | Nee |

### Item-parameters

Gebruikt binnen item-objecten (pr[0-9]{1,200}):

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `id` | string | Item ID | Nee |
| `nm` | string | Item Name | Nee |
| `br` | string | Item Brand | Nee |
| `ca` | string | Item Category Hierarchy 1 | Nee |
| `c2` | string | Item Category Hierarchy 2 | Nee |
| `c3` | string | Item Category Hierarchy 3 | Nee |
| `c4` | string | Item Category Hierarchy 4 | Nee |
| `c5` | string | Item Category Hierarchy 5 | Nee |
| `pr` | number | Item Price | Nee |
| `qt` | number | Item Quantity | Nee |
| `va` | string | Item Variant | Nee |
| `cp` | string | Item Coupon | Nee |
| `ds` | number | Item Discount | Nee |
| `ln` | string | Item List Name | Nee |
| `li` | string | Item List ID | Nee |
| `lp` | number | Item List Position | Nee |
| `af` | string | Item Affiliation | Nee |
| `lo` | string | Item Location ID | Nee |
| `cn` | string | Item Creative Name | Nee |
| `cs` | string | Item Creative Slot | Nee |
| `pi` | string | Item Promotion ID | Nee |
| `pn` | string | Item Promotion Name | Nee |

### Client-parameters

Client Hints-data:

| Parameter | Type | Beschrijving | Verplicht |
|-----------|------|-------------|----------|
| `uaa` | string | User Agent Architecture - bijv. "x86" | Nee |
| `uab` | string | User Agent Bitness - bijv. "64" | Nee |
| `uafvl` | string | User Agent Full Version List | Nee |
| `uamb` | bool | User Agent Mobile - geeft aan of het mobiel is | Nee |
| `uam` | string | User Agent Model - apparaatmodel | Nee |
| `uap` | string | User Agent Platform - bijv. "Windows" | Nee |
| `uapv` | string | User Agent Platform Version | Nee |

## Bronnen

- `https://datajournal.datakyu.co/ga4-api-reference/`
- `https://www.thyngster.com/ga4-measurement-protocol-cheatsheet/`
- `https://medium.com/@mssvarma06/ga4-measurement-protocol-parameter-reference-b63d87bbe0eb`
