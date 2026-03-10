# Matomo

This page is a concise, inference-based reference for the Matomo-compatible `/matomo.php` requests currently parsed by d8a.

## Method

`GET` or `POST`

`POST` may also send a batch body like:

```json
{"requests":["?idsite=1&url=https%3A%2F%2Fexample.com", "?idsite=1&e_c=Video&e_a=Play"]}
```

## URL

Common endpoint:

- `/matomo.php`

## Event type inference

The tracker payload is interpreted in this order:

| Condition | Mapped event name |
|---|---|
| `idgoal=0` and `ec_id` present | `ecommerce_order` |
| `idgoal` present | `goal_conversion` |
| `ma_id` present and `ma_mt=video` | `video_play` |
| `download` present | `download` |
| `link` present | `outlink` |
| `search` param present, even if empty | `site_search` |
| `c_i` present | `content_interaction` |
| `c_n` present | `content_impression` |
| `e_c` and `e_a` present | `event` |
| otherwise | `page_view` |

## Query parameters

### Core identity and routing

| Parameter | Probably mapped to | Notes |
|---|---|---|
| `idsite` | site ID / property ID | Required for routing in d8a. |
| `_id` | client ID | Primary client identifier when present. |
| `cid` | client ID fallback | Used only if `_id` is absent. |
| `uid` | user ID | Explicit user identifier. |
| `_idn` | returning-user flag | `0` means returning user; other/missing values behave like non-returning. |
| `lang` | device language | Fallback may also come from `Accept-Language`. |

### Page and navigation

| Parameter | Probably mapped to | Notes |
|---|---|---|
| `url` | page location | Also the source for nested `utm_*` and click IDs. |
| `action_name` | page title | Used on page views and other page-scoped hits. |
| `urlref` | page referrer | Empty string likely means direct / no referrer. |
| `ignore_referrer` | ignore-referrer flag | Alias: `ignore_referer`. `1` means true. |
| `pv_id` | page view ID | Kept as a string. |

### Events, goals, search, and content

| Parameter | Probably mapped to | Notes |
|---|---|---|
| `e_c` | event category | With `e_a`, makes an `event`. |
| `e_a` | event action | With `e_c`, makes an `event`. |
| `e_v` | event value | Numeric. |
| `idgoal` | goal ID | Any value makes `goal_conversion` unless ecommerce-order rule wins. |
| `link` | outbound link URL | Makes `outlink`. |
| `download` | download URL | Makes `download`. |
| `search` | site-search keyword | Presence alone makes `site_search`. |
| `search_cat` | site-search category | Optional. |
| `search_count` | site-search result count | Integer. |
| `c_i` | content interaction name | Makes `content_interaction`. |
| `c_n` | content name | Makes `content_impression` when `c_i` is absent. |
| `c_p` | content piece | Optional companion to `c_n` / `c_i`. |
| `c_t` | content target | Optional companion to `c_n` / `c_i`. |
| `ma_id` | media asset ID | Only observed as part of video detection. |
| `ma_mt` | media type | `video` plus `ma_id` makes `video_play`. |

### Ecommerce and product detail

| Parameter | Probably mapped to | Notes |
|---|---|---|
| `ec_id` | ecommerce order ID | With `idgoal=0`, treated as `ecommerce_order`. |
| `revenue` | purchase revenue | Grand total. |
| `ec_st` | subtotal | Excluding shipping. |
| `ec_tx` | tax | Numeric. |
| `ec_sh` | shipping | Numeric. |
| `ec_dt` | discount | Numeric. |
| `ec_items` | ecommerce items array | JSON array of item tuples. |
| `_pks` | product SKU | Product-detail style hit. |
| `_pkn` | product name | Product-detail style hit. |
| `_pkp` | product price | Numeric. |
| `_pkc` | product categories | JSON array or single raw string. |

`ec_items` is parsed as tuples in this shape:

```text
[sku, name, category, price, quantity]
```

The category slot may be either a single string or an array of up to 5 category levels.

### Custom data

| Parameter | Probably mapped to | Notes |
|---|---|---|
| `cvar` | event custom variables | JSON object like `{"1":["name","value"]}`. |
| `_cvar` | session custom variables | Same JSON shape as `cvar`; merged across session. |
| `dimensionN` | custom dimension slot `N` | Examples: `dimension1`, `dimension2`. |

### Marketing params carried inside `url`

These are not top-level Matomo query params in d8a. They are extracted from the page URL inside `url`.

| Parameter in `url` query | Probably mapped to |
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

## Practical read

If you only need the useful mental model, the Matomo tracker mostly sends:

- page context via `url`, `action_name`, `urlref`
- identity via `idsite`, `_id` or `cid`, optionally `uid`
- event semantics via `e_*`, `link`, `download`, `search`, `idgoal`, `c_*`
- ecommerce via `ec_*` and product detail via `_pk*`
- custom metadata via `cvar`, `_cvar`, and `dimensionN`
