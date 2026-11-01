# Event transformations

Event transformations change or discard events as they arrive, before anything is
stored. Use them to:

- **protect privacy:** remove or mask personal data, exclude your own traffic;
- **keep reports clean:** fix event names, group similar pages, merge campaign sources;
- **exclude unwanted traffic:** test sites, tag-manager previews, debug events.

Because transformations run before storage, data they remove or mask is never kept.
They apply to new events only; data you already collected is not changed.

## Quick start

Add a `transformations` section to your configuration file and start the tracker with it:

```sh
./tracker-api run --config config.yaml
```

```yaml
transformations:
  rules:
    - name: Exclude internal traffic
      expression: 'inCidr(ip_address, "203.0.113.0/24")'
      actions:
        - type: drop
```

Each rule has a **name**, an **expression** that decides which events it applies to (`true`
for all events), and one or more **actions**. Invalid configuration is reported when the
tracker starts.

## Building a rule

### Choosing events

Expressions use the same language as [traffic filtering](traffic-filtering.md):

| You want | Expression |
| --- | --- |
| all events | `true` |
| one event | `name == "purchase"` |
| several events | `name in ["purchase", "refund"]` |
| a part of the site | `page_path != nil && page_path startsWith "/blog"` |
| a custom parameter | `params["environment"] == "test"` |
| a number parameter | `numeric_params["value"] != nil && numeric_params["value"] > 1000` |
| a page URL parameter | `url_params["utm_source"] == "newsletter"` |
| an IP range | `inCidr(ip_address, "10.0.0.0/8")` |
| a pattern | `page_title matches "(?i)error"` |

Fields without a value are `nil`. When a missing value must not match, check for it first:
`page_title != nil && page_title != "Home"`.

### Actions

| Action | What it does | Example |
| --- | --- | --- |
| `set` | Sets a field to a value. | `{type: set, target: {field: name}, value: {string: checkout}}` |
| `set` with a template | Builds a value from other fields. | `value: {template: "{{page_title}} - {{params.section}}"}` |
| `remove` | Removes a field or parameter. | `{type: remove, target: {field: user_id}}` |
| `replace` | Replaces text matching a pattern anywhere in a field. | `{type: replace, target: {field: page_location}, pattern: "\\d{6,}"}` |
| `drop` | Discards the event. It must be the only action in its rule. | `{type: drop}` |

Targets are written as `{field: <name>}`, `{parameter: <name>, type: string|number}` for
custom event parameters, or `{url_parameter: <name>}` for a query parameter of the page URL.

**Templates** insert values with `{{field}}`, `{{params.name}}`, `{{numeric_params.name}}`
or `{{url_params.name}}`. A template that is just one missing value leaves the target
unchanged.

**Replace** uses a regular expression and replaces every match with `replacement`, which
defaults to `[REDACTED]`. Use an empty replacement to delete matches, and `$1` to keep a
matched group (for example `/orders/123` → `/orders/:id`).

## Things to know

- **Order matters.** Rules run from top to bottom, and each rule sees the changes made by
  the rules above it. Within one rule, every action reads the event as it was before that
  rule, so the actions don't affect each other.
- **One change per field per rule.** To change a field twice, use two rules. A rule can't
  change the page URL together with its parts (path, domain, URL parameters).
- **`ip_address` is the full address before IP masking.** It can only be used to choose
  events, so office ranges can be excluded even when masking is on. Location fields are
  looked up from the masked address, as in your reports.
- **Location fields can be overwritten individually.** Changing the country doesn't change
  the region or city; set or remove those too if needed.
- **Not available:** browser, operating system and device fields, and session fields such
  as landing page or session source.
- **Matomo** events can't be renamed, and custom event parameters apply to GA4 and Native
  tracking only. Actions that a protocol can't apply are skipped for that protocol.
- **Errors:** if a rule can't be applied to an event (for example a malformed page URL), that
  event is discarded rather than stored unchanged, so a privacy rule can never be bypassed.
- **Speed:** keep conditions on event names and parameters first: `name == "purchase" &&
  geo_country == "NL"` looks up the location only for purchases. Patterns run only on events
  a rule applies to; simple, specific patterns are the fastest.
- **Patterns** use [RE2 syntax](https://github.com/google/re2/wiki/Syntax): lookarounds
  (`(?=…)`) and backreferences (`\1`) aren't supported. Add `(?i)` for case-insensitive
  matching.

## Fields

| Group | Fields | Can be changed |
| --- | --- | --- |
| Event | `name`, `user_id` | yes (event name: GA4 and Native) |
| Page | `page_location`, `page_title`, `page_referrer`, `page_hostname`, `page_path` | yes |
| Traffic source | `utm_source`, `utm_medium`, `utm_campaign`, `utm_term`, `utm_content`, other `utm_*`, click IDs such as `click_id_gclid` | yes |
| Location | `geo_country`, `geo_region`, `geo_city`, `geo_continent` | yes |
| Request | `ip_address` | no, conditions only |
| Parameters | any custom event parameter, any page URL parameter | yes |
| Other | e.g. `device_language`, `device_screen_resolution`, e-commerce values | some |

Field names are the same as in traffic filters.

## Recipes

### Privacy

**Exclude internal traffic.** Replace the range with your office or VPN addresses.

```yaml
transformations:
  rules:
    - name: Exclude internal traffic
      expression: 'inCidr(ip_address, "203.0.113.0/24") || inCidr(ip_address, "198.51.100.7/32")'
      actions:
        - type: drop
```

**Redact personal data anywhere in page URLs, titles and referrers.** Masks email
addresses (also URL-encoded ones), international phone numbers, card numbers and IP
addresses. Card numbers are recognized as any sequence of 13–19 digits; because a pattern
can't tell them apart from other long numbers, long order or tracking numbers are masked as well.

```yaml
transformations:
  rules:
    - name: Redact personal data
      expression: 'true'
      actions:
        - type: replace
          target: {field: page_location}
          pattern: '(?i:[a-z0-9._%+-]+(?:@|%40)[a-z0-9.-]+\.[a-z]{2,})|(?:\+|%2B)\d(?:[ .()-]?\d){7,14}|\d(?:[ -]?\d){12,18}|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        - type: replace
          target: {field: page_title}
          pattern: '(?i:[a-z0-9._%+-]+(?:@|%40)[a-z0-9.-]+\.[a-z]{2,})|(?:\+|%2B)\d(?:[ .()-]?\d){7,14}'
        - type: replace
          target: {field: page_referrer}
          pattern: '(?i:[a-z0-9._%+-]+(?:@|%40)[a-z0-9.-]+\.[a-z]{2,})'
```

**Redact personal data in site search terms.**

```yaml
transformations:
  rules:
    - name: Redact search terms
      expression: 'params["search_term"] != nil'
      actions:
        - type: replace
          target: {parameter: search_term, type: string}
          pattern: '(?i:[a-z0-9._%+-]+(?:@|%40)[a-z0-9.-]+\.[a-z]{2,})|(?:\+|%2B)\d(?:[ .()-]?\d){7,14}'
```

**Reduce location precision.** Keep the country only.

```yaml
transformations:
  rules:
    - name: Country-level location only
      expression: 'true'
      actions:
        - type: remove
          target: {field: geo_city}
        - type: remove
          target: {field: geo_region}
```

**Don't store user IDs.**

```yaml
transformations:
  rules:
    - name: No user IDs
      expression: 'true'
      actions:
        - type: remove
          target: {field: user_id}
```

**Remove ad click IDs.** Visits can no longer be matched to ad clicks.

```yaml
transformations:
  rules:
    - name: No ad click IDs
      expression: 'true'
      actions:
        - type: remove
          target: {field: click_id_gclid}
        - type: remove
          target: {field: click_id_fbclid}
        - type: remove
          target: {field: click_id_msclkid}
```

**Remove a sensitive URL parameter** when you know its name.

```yaml
transformations:
  rules:
    - name: Remove reset tokens
      expression: 'url_params["token"] != nil'
      actions:
        - type: remove
          target: {url_parameter: token}
```

### Cleaner reports

**Group pages that contain IDs**, so `/orders/123` and `/orders/456` are reported together
as `/orders/:id`.

```yaml
transformations:
  rules:
    - name: Group pages with IDs
      expression: 'true'
      actions:
        - type: replace
          target: {field: page_location}
          pattern: '(/)(?:\d+|(?i:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}))(/|\?|#|$)'
          replacement: '$1:id$2'
```

**Remove the site name from page titles.**

```yaml
transformations:
  rules:
    - name: Shorter page titles
      expression: 'page_title != nil'
      actions:
        - type: replace
          target: {field: page_title}
          pattern: '\s*[|–-]\s*Example Shop$'
          replacement: ''
```

**Remove query strings from referrers.**

```yaml
transformations:
  rules:
    - name: Clean referrers
      expression: 'page_referrer != nil'
      actions:
        - type: replace
          target: {field: page_referrer}
          pattern: '\?.*$'
          replacement: ''
```

**Merge campaign sources.**

```yaml
transformations:
  rules:
    - name: One Facebook source
      expression: 'utm_source in ["fb", "facebook.com", "m.facebook.com"]'
      actions:
        - type: set
          target: {field: utm_source}
          value: {string: facebook}
```

**Rename an event**, for example after changing it in your tracking code (GA4 and Native).

```yaml
transformations:
  rules:
    - name: Rename sign-up event
      expression: 'name == "signup"'
      actions:
        - type: set
          target: {field: name}
          value: {string: sign_up}
```

**Keep the original value** in a parameter before changing it.

```yaml
transformations:
  rules:
    - name: Generic checkout title
      expression: 'page_path != nil && page_path startsWith "/checkout"'
      actions:
        - type: set
          target: {parameter: original_title, type: string}
          value: {template: '{{page_title}}'}
        - type: set
          target: {field: page_title}
          value: {string: Checkout}
```

**Tag events from a part of the site.**

```yaml
transformations:
  rules:
    - name: Blog section
      expression: 'page_path != nil && page_path startsWith "/blog"'
      actions:
        - type: set
          target: {parameter: section, type: string}
          value: {string: blog}
```

**Flag large orders** using a number parameter.

```yaml
transformations:
  rules:
    - name: Flag large orders
      expression: 'name == "purchase" && numeric_params["value"] != nil && numeric_params["value"] >= 1000'
      actions:
        - type: set
          target: {parameter: large_order, type: string}
          value: {string: "yes"}
```

### Excluding traffic

**Drop test and staging sites.**

```yaml
transformations:
  rules:
    - name: Drop test sites
      expression: 'page_hostname in ["localhost", "staging.example.com"] || (page_hostname != nil && page_hostname endsWith ".test")'
      actions:
        - type: drop
```

**Drop Google Tag Manager preview traffic.**

```yaml
transformations:
  rules:
    - name: Drop Tag Manager previews
      expression: 'gtm_debug != nil'
      actions:
        - type: drop
```

**Drop events marked as test events.**

```yaml
transformations:
  rules:
    - name: Drop test events
      expression: 'params["environment"] == "test"'
      actions:
        - type: drop
```

## Limits

- 100 rules, with up to 20 actions each.
- Names and parameter names: up to 100 characters.
- Values and templates: up to 8,192 characters. Expressions: up to 16,384 characters.
- Patterns: up to 2,000 characters. Fields longer than 16,384 characters aren't searched.
