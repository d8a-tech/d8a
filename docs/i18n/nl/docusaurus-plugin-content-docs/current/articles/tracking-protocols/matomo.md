# Matomo

Deze pagina is een beknopte, op inferentie gebaseerde referentie voor de Matomo-compatibele `/matomo.php`-verzoeken die d8a momenteel parseert.

## Methode

`GET` of `POST`

`POST` kan ook een batch-body verzenden zoals:

```json
{"requests":["?idsite=1&url=https%3A%2F%2Fexample.com", "?idsite=1&e_c=Video&e_a=Play"]}
```

## URL

Veelvoorkomend endpoint:

- `/matomo.php`

## Inferentie van het event-type

De tracker-payload wordt in deze volgorde geïnterpreteerd:

| Voorwaarde | Toegewezen event name |
|---|---|
| `idgoal=0` en `ec_id` aanwezig | `ecommerce_order` |
| `idgoal` aanwezig | `goal_conversion` |
| `ma_id` aanwezig en `ma_mt=video` | `video_play` |
| `download` aanwezig | `download` |
| `link` aanwezig | `outlink` |
| `search`-param aanwezig, zelfs indien leeg | `site_search` |
| `c_i` aanwezig | `content_interaction` |
| `c_n` aanwezig | `content_impression` |
| `e_c` en `e_a` aanwezig | `event` |
| anders | `page_view` |

## Queryparameters

### Kern-identiteit en routing

| Parameter | Waarschijnlijk toegewezen aan | Opmerkingen |
|---|---|---|
| `idsite` | site ID / property ID | Vereist voor routing in d8a. |
| `_id` | client ID | Primaire client-identifier indien aanwezig. |
| `cid` | client ID-fallback | Alleen gebruikt als `_id` ontbreekt. |
| `uid` | user ID | Expliciete gebruikersidentifier. |
| `_idn` | terugkerende-gebruiker-vlag | `0` betekent terugkerende gebruiker; andere/ontbrekende waarden gedragen zich als niet-terugkerend. |
| `lang` | apparaattaal | Fallback kan ook uit `Accept-Language` komen. |

### Pagina en navigatie

| Parameter | Waarschijnlijk toegewezen aan | Opmerkingen |
|---|---|---|
| `url` | page location | Ook de bron voor geneste `utm_*` en click-id's. |
| `action_name` | page title | Gebruikt bij page views en andere pagina-scoped hits. |
| `urlref` | page referrer | Een lege string betekent waarschijnlijk direct / geen referrer. |
| `ignore_referrer` | ignore-referrer-vlag | Alias: `ignore_referer`. `1` betekent true. |
| `pv_id` | page view ID | Bewaard als string. |

### Events, goals, search en content

| Parameter | Waarschijnlijk toegewezen aan | Opmerkingen |
|---|---|---|
| `e_c` | event category | Vormt met `e_a` een `event`. |
| `e_a` | event action | Vormt met `e_c` een `event`. |
| `e_v` | event value | Numeriek. |
| `idgoal` | goal ID | Elke waarde vormt `goal_conversion`, tenzij de ecommerce-order-regel wint. |
| `link` | URL van uitgaande link | Vormt `outlink`. |
| `download` | download-URL | Vormt `download`. |
| `search` | zoekwoord site-search | Aanwezigheid alleen vormt al `site_search`. |
| `search_cat` | categorie site-search | Optioneel. |
| `search_count` | aantal resultaten site-search | Integer. |
| `c_i` | naam content-interactie | Vormt `content_interaction`. |
| `c_n` | content name | Vormt `content_impression` wanneer `c_i` ontbreekt. |
| `c_p` | content piece | Optionele aanvulling op `c_n` / `c_i`. |
| `c_t` | content target | Optionele aanvulling op `c_n` / `c_i`. |
| `ma_id` | media asset ID | Alleen waargenomen als onderdeel van videodetectie. |
| `ma_mt` | media type | `video` plus `ma_id` vormt `video_play`. |

### Ecommerce en productdetails

| Parameter | Waarschijnlijk toegewezen aan | Opmerkingen |
|---|---|---|
| `ec_id` | ecommerce order ID | Met `idgoal=0` behandeld als `ecommerce_order`. |
| `revenue` | aankoopomzet | Eindtotaal. |
| `ec_st` | subtotaal | Exclusief verzending. |
| `ec_tx` | btw | Numeriek. |
| `ec_sh` | verzending | Numeriek. |
| `ec_dt` | korting | Numeriek. |
| `ec_items` | array met ecommerce-items | JSON-array van item-tuples. |
| `_pks` | product-SKU | Hit in productdetail-stijl. |
| `_pkn` | productnaam | Hit in productdetail-stijl. |
| `_pkp` | productprijs | Numeriek. |
| `_pkc` | productcategorieën | JSON-array of enkele ruwe string. |

`ec_items` wordt geparseerd als tuples in deze vorm:

```text
[sku, name, category, price, quantity]
```

De category-slot kan ofwel een enkele string zijn ofwel een array van maximaal 5 categorieniveaus.

### Custom data

| Parameter | Waarschijnlijk toegewezen aan | Opmerkingen |
|---|---|---|
| `cvar` | event custom variables | JSON-object zoals `{"1":["name","value"]}`. |
| `_cvar` | session custom variables | Dezelfde JSON-vorm als `cvar`; samengevoegd over de session. |
| `dimensionN` | custom dimension-slot `N` | Voorbeelden: `dimension1`, `dimension2`. |

### Marketingparameters meegegeven binnen `url`

Dit zijn geen Matomo-queryparameters op het hoogste niveau in d8a. Ze worden uit de page-URL binnen `url` gehaald.

| Parameter in `url`-query | Waarschijnlijk toegewezen aan |
|---|---|
| `utm_campaign` | campaign |
| `utm_source` | source |
| `utm_medium` | medium |
| `utm_content` | content |
| `utm_term` | term |
| `utm_id` | campaign ID |
| `utm_source_platform` | source platform |
| `utm_creative_format` | creative format |
| `utm_marketing_tactic` | marketing tactic |
| `gclid` | Google click ID |
| `dclid` | Google Display click ID |
| `gbraid` | Google braid click ID |
| `wbraid` | Google web braid click ID |
| `fbclid` | Meta click ID |
| `msclkid` | Microsoft click ID |
| `srsltid` | Google Shopping result click ID |

## Praktische samenvatting

Als je alleen het bruikbare mentale model nodig hebt: de Matomo-tracker verzendt grotendeels:

- pagina-context via `url`, `action_name`, `urlref`
- identiteit via `idsite`, `_id` of `cid`, optioneel `uid`
- event-semantiek via `e_*`, `link`, `download`, `search`, `idgoal`, `c_*`
- ecommerce via `ec_*` en productdetails via `_pk*`
- custom metadata via `cvar`, `_cvar` en `dimensionN`
