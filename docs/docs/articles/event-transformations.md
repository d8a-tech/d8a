# Event transformations

Event transformations rename, set, remove or drop events in the receiver, before they
are queued, logged or stored. Removed and dropped data never reaches the queue, the raw
log or the warehouse.

## Where they run

The receiver executes configured transformations in this order: protocol parsing →
per-hit processing (IP masking) → transformations → retained-hit validation → raw
logging → queueing. Invalid configuration fails startup.

For configured traffic, only retained events are persisted, as normalized per-event
requests with the transformed parameters and no original batch body. Normalized
records are not byte-identical HTTP requests and are not guaranteed to replay through
legacy parsers. An omitted configuration or an empty `rules` list keeps legacy raw
logging.

## YAML configuration

Save this as `config.yaml` and load it with the existing flag:

```sh
./tracker-api run --config config.yaml
```

Rules are stored in the default property settings alongside traffic filters. Unknown
keys inside `transformations` are rejected; invalid predicates or actions fail
startup rather than being ignored.

```yaml
transformations:
  rules:
    - name: Rename checkout events
      expression: '(name == "checkout" && page_path == "/checkout") || name == "begin_checkout"'
      actions:
        - type: set
          target: {field: name}
          value: {string: checkout_started}
        - type: set
          target: {parameter: score, type: number}
          value: {number: 1}
    - name: Keep the original title in a parameter
      expression: 'page_title != nil && name == "checkout_started"'
      actions:
        - type: set
          target: {parameter: original_title, type: string}
          value: {template: "{{page_title}}"}
        - type: set
          target: {field: page_title}
          value: {string: Checkout}
    - name: Tag newsletter traffic
      expression: 'utm_source == "newsletter"'
      actions:
        - type: set
          target: {field: utm_campaign}
          value: {template: "{{utm_campaign}}-{{url_params.edition}}"}
    - name: Remove a custom parameter
      expression: 'true'
      actions:
        - type: remove
          target: {parameter: debug, type: string}
    - name: Drop office traffic
      expression: 'inCidr(ip_address, "203.0.113.0/24")'
      actions:
        - type: drop
```

## Fields

Field names are the event column names, the same names traffic filters use (for
example `name`, `page_location`, `utm_source`, `geo_country`). The catalog is derived
from the protocols' event columns; `transformations.Catalog()` returns it with each
field's type, category and the protocols that can read, write and remove it.

| Fields | Read | Write (`set`) | Remove |
| --- | --- | --- | --- |
| `name` | GA4, Native, Matomo | GA4, Native | No |
| `page_location`, `page_title`, `page_referrer`, `user_id` | All | All | Yes |
| `page_hostname`, `page_path` | All | All (rewrites the page URL) | No |
| `utm_*`, `click_id_*` | All | All (rewrites the page URL) | Yes |
| `geo_continent`, `geo_country`, `geo_region`, `geo_city` | All | All (override) | Yes |
| Other columns read from one request parameter (e.g. `device_screen_resolution`) | Per protocol | Per protocol | Yes |
| Other event columns (e.g. `device_language`, `platform`) | Per protocol | No | No |
| `ip_address` (unmasked, conditions only) | All | No | No |
| Custom event parameters | GA4, Native | GA4, Native | Yes |
| Page URL query parameters | All | All | Yes |

- **Custom event parameters** are `params["name"]` (string) and
  `numeric_params["name"]` (number) in expressions, and `{parameter: name, type:
  string|number}` in actions. Any name works; `transformations.KnownParameters()`
  lists the ones GA4 already maps to columns, as suggestions.
- **Page URL query parameters** are `url_params["name"]` in expressions and
  `{url_parameter: name}` in actions. They are read from the page URL as sent, before
  URL exclusions, like the `utm_*` columns.
- **`ip_address`** is the client IP as received, before IP masking, so office ranges
  can be matched with `inCidr` even when masking is on. It is available in conditions
  only: it cannot be written or used in templates, so it never ends up in stored data.
  Traffic filters' `ip_address` is the stored, masked column.
- **Geo overrides** apply only to the field written. Changing the country does not
  clear the region or city; write or remove those as well if needed. Geolocation is
  looked up from the masked IP, as in stored data. Overrides travel with the hit and
  are not part of the raw log.
- **Device detection fields** (browser, operating system, device category, brand,
  model) are not available: parsing user agents for every incoming hit is too
  expensive.
- Session-derived fields are not available: sessions do not exist yet in the receiver.
- Wire aliases (`en`, `dl`/`url`, `dt`/`action_name`, `dr`/`urlref`, `uid`) are
  accepted in action targets only.

## Expressions

The engine uses the same expr-lang language and `inCidr(string, string)` function as
[traffic filtering](traffic-filtering.md).

- Missing and empty values are `nil` for fields. Custom and URL parameters that are
  present but empty are `""`. Traffic filters instead see `""` for missing fields, so
  compare with `nil` here: `user_id == nil`.
- Guard comparisons when absence must not match:
  `page_title != nil && page_title != "Internal"`. A match-all rule uses `true`.
- Fields are extracted lazily, so `&&` and `||` stop early. Put cheap conditions
  (event name, parameters) before geo or URL conditions:
  `name == "purchase" && geo_country == "NL"` looks up geolocation only for purchases.
- Predicates must return a boolean; type errors and invalid constant regular
  expressions are rejected when compiling.

## Actions

- `set` takes exactly one value:
  - `{string: "text"}` or `{number: 0}` literals, matching the target type;
  - `{template: "..."}`, where `{{page_title}}`, `{{params.x}}`, `{{numeric_params.x}}`
    and `{{url_params.x}}` insert values and `\{{` is a literal `{{`. Missing values
    become empty text. A template that is a single reference to a missing value leaves
    the target unchanged. Number targets need a template with exactly one number
    reference.
- `remove` deletes the target. Removing a missing target is a no-op. Removing a
  custom parameter removes its selected typed representation; setting one replaces
  both its `ep.` and `epn.` forms.
- `drop` must be the only action in its rule and stops processing for the event.
- Rules run in order; later rules see earlier writes. Within a rule, all values are
  computed from the state before the rule, then written in order, so swapping two
  fields in one rule works.
- A target may appear once per rule. `page_location` cannot be written together with
  URL parts (`page_hostname`, `page_path`, `utm_*`, URL parameters) in the same rule.
- Actions for fields a protocol cannot write are skipped for that protocol.
- The previous `copy` action is replaced by `set` with a template:
  `{template: "{{page_title}}"}`.

## Errors

A rule that fails at runtime (for example a malformed page URL read through
`page_path`, or a failing geolocation lookup) drops only the affected event: passing it
on untransformed could keep data the rule is meant to remove. Every failure increments
the `receiver.transformations.errors` counter (attributes `stage`: extraction,
predicate, action or compile, and `rule`: the 1-based rule index) and is logged without
payload, rate-limited.

Limits: 100 rules, 20 actions per rule, 100 characters per rule or parameter name,
8,192 characters per string literal or template and 16,384 characters per expression.

## Engine integration API

```go
program, err := transformations.Compile(config) // properties.TransformationsConfig
if err != nil {
    return err
}
keep, err := program.Apply(transformations.Input{
    Protocol:   protocolID,
    Hit:        hit,
    Settings:   settings,
    OriginalIP: request.IP, // before masking; used only for ip_address conditions
    Dependencies: transformations.ExtractionDependencies{
        Columns: map[string]schema.ColumnsRegistry{protocolID: columnsRegistry},
    },
})
```

`Compile` returns an immutable `transformations.Program` that is safe for concurrent
use; `Apply` needs exclusive ownership of its hit and returns a payload-free
`*transformations.Error` with the failing stage and rule. Reads run the same event
column implementations the worker uses, so pass the worker's column registries.
`receiver.NewTransformationBatchProcessingRule` wraps this for the receiver: it
captures one configuration per property per batch, memoizes compiled programs,
normalizes retained requests and drops failing hits.
