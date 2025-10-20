# GA4

This is a reference of reverse-engineered `/g/collect` AKA gtag protocol.

:::info
    3rd party references used:
        * https://datajournal.datakyu.co/ga4-api-reference/
        * https://www.thyngster.com/ga4-measurement-protocol-cheatsheet/
:::

## Method

`POST`

## URL

`https://www.google-analytics.com/g/collect`


## Query Parameters

### Request Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `v` | string | Protocol Version | Yes |
| `tid` | string | Tracking/Property ID - GA4 property identifier (e.g., G-XXXXXXXXXX) | Yes |
| `gtm` | string | GTM Has Info - indicates Google Tag Manager information | No |
| `_p` | int64 | Random Page Load Hash - UTC Unix timestamp of the page load, in milliseconds | Yes |
| `sr` | string | Screen Resolution - e.g., "1920x1080" | No |
| `ul` | string | User Language - e.g., "en-us" | No |
| `dh` | string | Document Hostname - hostname of the page | No |
| `cid` | string | Client ID - unique identifier for the client | Yes |
| `_s` | int64 | Hit Counter - indicates this is the nth hit/event sent in the current session | No |
| `richsstsse` | string | richsstsse parameter | No |

### Event Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `en` | string | Event Name - name of the event being tracked | Yes |
| `_et` | int64 | Engagement Time - time user engaged with the page in milliseconds | No |
| `ep.*` | string | Event Parameter (String) - custom event parameter | No |
| `epn.*` | number | Event Parameter (Number) - custom numeric event parameter | No |
| `_c` | bool | is Conversion - indicates if event is a conversion | No |
| `_ee` | bool | External Event - indicates if event is external | No |

### Shared Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `dl` | string | Document Location - URL of the page | No |
| `dt` | string | Document Title - title of the page | No |
| `dr` | string | Document Referrer - referrer URL | No |
| `_eu` | string | Event Usage | No |
| `_edid` | string | Event Debug ID | No |
| `_dbg` | bool | is Debug - indicates debug mode | No |
| `ir` | bool | Ignore Referrer - if present and true, referrer is ignored. [GA4 docs](https://support.google.com/analytics/answer/10327750?hl=en) | No |
| `tt` | string | Traffic Type | No |
| `gcs` | string | Google Consent Status | No |
| `gcu` | string | Google Consent Update | No |
| `gcut` | string | Google Consent Update Type | No |
| `gcd` | string | Google Consent Default | No |
| `_glv` | bool | is Google Linker Valid | No |

### E-commerce Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `cu` | string | Currency Code - e.g., "USD", "EUR" | No |
| `ep.affiliation` | string | Affiliation - store or affiliation | No |
| `epn.value` | number | Transaction Revenue - total transaction value | No |
| `epn.tax` | number | Transaction Tax | No |
| `epn.shipping` | number | Transaction Shipping | No |
| `pr[0-9]{1,200}` | object | Item - product/item data (supports up to 200 items) | No |
| `pi` | string | Promotion ID | No |
| `pn` | string | Promotion Name | No |
| `cn` | string | Creative Name | No |
| `cs` | string | Creative Slot | No |
| `li` | string | Location ID | No |

### Campaign Attribution Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `cm` | string | Campaign Medium - e.g., "cpc", "email" | No |
| `cs` | string | Campaign Source - e.g., "google", "newsletter" | No |
| `cn` | string | Campaign Name | No |
| `cc` | string | Campaign Content | No |
| `ct` | string | Campaign Term | No |
| `ccf` | string | Campaign Creative Format | No |
| `cmt` | string | Campaign Marketing Tactic | No |
| `_rnd` | string | GCLID Deduper | No |

### User & Session Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `uid` | string | User ID - optional user identifier | No |
| `_fid` | string | Firebase ID | No |
| `sid` | string | Session ID - unique session, assigned by the client when session starts, then propagated to every event | Yes |
| `sct` | int64 | Session Count - number of sessions for the user | No |
| `seg` | int64 | Session Engagement - engagement level of the session | No |
| `up.*` | string | User Property(ies) - custom user properties | No |
| `upn.*` | number | User Property Value - numeric user property | No |
| `_fv` | bool | First Visit - indicates first visit | No |
| `_ss` | bool | Session Start - indicates that this event starts the session. If present, always has value `1` | No |
| `_fplc` | string | First Party Linker Cookie | No |
| `nsi` | string | New Session ID | No |
| `gdid` | string | Google Developer ID | No |
| `_uc` | string | User Country | No |

### Item Parameters

Used within item objects (pr[0-9]{1,200}):

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `id` | string | Item ID | No |
| `nm` | string | Item Name | No |
| `br` | string | Item Brand | No |
| `ca` | string | Item Category Hierarchy 1 | No |
| `c2` | string | Item Category Hierarchy 2 | No |
| `c3` | string | Item Category Hierarchy 3 | No |
| `c4` | string | Item Category Hierarchy 4 | No |
| `c5` | string | Item Category Hierarchy 5 | No |
| `pr` | number | Item Price | No |
| `qt` | number | Item Quantity | No |
| `va` | string | Item Variant | No |
| `cp` | string | Item Coupon | No |
| `ds` | number | Item Discount | No |
| `ln` | string | Item List Name | No |
| `li` | string | Item List ID | No |
| `lp` | number | Item List Position | No |
| `af` | string | Item Affiliation | No |
| `lo` | string | Item Location ID | No |
| `cn` | string | Item Creative Name | No |
| `cs` | string | Item Creative Slot | No |
| `pi` | string | Item Promotion ID | No |
| `pn` | string | Item Promotion Name | No |

### Client Parameters

Client Hints data:

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `uaa` | string | User Agent Architecture - e.g., "x86" | No |
| `uab` | string | User Agent Bitness - e.g., "64" | No |
| `uafvl` | string | User Agent Full Version List | No |
| `uamb` | bool | User Agent Mobile - indicates if mobile | No |
| `uam` | string | User Agent Model - device model | No |
| `uap` | string | User Agent Platform - e.g., "Windows" | No |
| `uapv` | string | User Agent Platform Version | No |
