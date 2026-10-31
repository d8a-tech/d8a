# Event transformations

## Implementation status

The receiver executes configured transformations in this order: protocol parsing →
existing per-hit processing/IP masking → transformations → retained-hit validation →
raw logging → queueing. Invalid configuration fails startup.

For configured traffic, only retained events are persisted as normalized per-event
requests with effective transformed parameters and no original batch bodies.
Normalized records are not byte-identical HTTP requests and do not promise legacy
parser replay compatibility. An omitted transformation configuration or empty rules
list preserves legacy raw logging.

## YAML configuration

Save this as `config.yaml` and load it with the existing flag:

```sh
./tracker-api run --config config.yaml
```

The complete example below is accepted and validated at startup. Rules are stored
in the default property settings alongside traffic filters. No additional flag or
provider is required. An omitted section or empty `rules` list is a no-op.
Unknown keys inside `transformations` are rejected; unrelated top-level configuration
is allowed. Invalid predicates/actions fail startup rather than being ignored.

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
    - name: Preserve the current title
      expression: 'page_title != nil && name == "checkout_started"'
      actions:
        - type: copy
          source: {field: page_title}
          target: {parameter: original_title, type: string}
        - type: set
          target: {field: page_title}
          value: {string: Checkout}
    - name: Remove a custom parameter
      expression: 'true'
      actions:
        - type: remove
          target: {parameter: debug, type: string}
    - name: Drop test events
      expression: 'params.environment != nil && params.environment == "test"'
      actions:
        - type: drop
```

## Expression environment

The engine uses the same expr-lang language and `inCidr(string, string)` function
as [traffic filtering](traffic-filtering.md). Existing traffic-filter semantics are
unchanged. Unlike traffic filters, transformations expose only this early catalog:

| Fields | Read | Set/copy target | Remove |
| --- | --- | --- | --- |
| `name` | GA4, Native, Matomo | GA4, Native | No |
| `page_location`, `page_title`, `page_referrer`, `user_id` | All three | All three | Yes |
| `page_hostname`, `page_path` | All three | No | No |
| `device_web_browser`, `device_operating_system`, `device_category` | All three | No | No |
| `geo_country`, `geo_region`, `geo_city` | All three | No | No |
| Named string/number event parameters | GA4, Native | GA4, Native | Yes |

Use friendly canonical names in expressions, `params["custom_name"]` for strings
and `numeric_params["score"]` for numbers. Parameter names are literal keys, not
executable expressions. Common wire aliases (`en`, `dl`/`url`, `dt`/`action_name`,
`dr`/`urlref`, `uid`) are accepted only in action field references and normalized
before duplicate checking. Expressions use canonical names, not aliases.

Missing values are `nil`; an explicitly present empty string is not missing.
Guard ordinary comparisons, including negative comparisons, when absence must not
match: `page_title != nil && page_title != "Internal"`. Exists/not-exists are
`page_title != nil` / `page_title == nil`. A match-all rule uses `true`.
Predicates must return boolean; type errors and invalid constant regular expressions
are rejected when compiling. Runtime evaluation/extraction errors return a
payload-free error, not a successful bypass.

No raw hit, body, headers, timestamps, routing identifiers, IP writes, session-derived
fields, filesystem or network callbacks are available in expressions. `inCidr` can
operate on a supported string field/parameter, not an exposed raw IP variable.
Device detection shares downstream UA/client-hint parsing and caching. Geolocation
uses the explicitly injected lookup provider and the already masked request IP;
unavailable data is absent, whereas lookup failures abort evaluation. Passing the
existing geo column factory shares its bounded lookup cache. Page location uses
the property's URL exclusions; hostname/path derive from that same cleaned location.

## Action and execution semantics

- Rules execute in array order. Later rules see earlier writes and may overwrite
  the same target. Derived page values are invalidated after location changes.
- Copy sources are all resolved from the pre-rule snapshot. Swapping title and
  referrer in one rule therefore works. Source extraction must succeed before any
  action in the rule is applied.
- `set` accepts exactly one typed literal: `{string: "value"}` or `{number: 0}`.
  Numbers must be finite; strings and numbers are never coerced into one another.
- `copy` requires matching source/target types. Missing or protocol-unsupported
  sources skip the action. Literal sets create missing targets.
- `remove` of a missing target is a no-op. A custom parameter reference removes
  only its selected typed representation. Setting/copying a parameter removes
  its conflicting `ep.`/`epn.` representation.
- Duplicate canonical targets within one rule are invalid, including aliases and
  both typed representations of one custom parameter. Drop must be the only
  action in its rule and immediately returns `keep=false` without an error.
- Unsupported protocol actions are skipped. Matomo has no generic event-parameter
  editing and its synthesized event name is read-only. Common Matomo writes use
  `url`, `action_name`, `urlref`, and `uid`. GA4/Native use `dl`, `dt`, `dr`, `uid`,
  `en`, `ep.<key>` and `epn.<key>`. Name/user ID writes also update the parsed hit.

Limits: 100 rules, 20 actions per rule, 100 characters per rule/parameter name,
8,192 characters per string literal and 16,384 characters per expression. The
internal executable-config cache holds at most 256 immutable programs; rule names
do not affect its fingerprint. No organization/revision/editor data enters OSS settings.

## Engine integration API

```go
program, err := transformations.Compile(config) // properties.TransformationsConfig
if err != nil {
    return err
}
keep, err := program.Apply(protocolID, hit, settings,
    transformations.ExtractionDependencies{Geo: geoLookupProvider})
```

`Compile` returns the `transformations.Program` interface. Programs are immutable
and safe for concurrent use, but `Apply` needs exclusive ownership of its hit.
The caller must discard the affected request on error: previous successful rules
may already have changed it. Catalog metadata is available through
`transformations.Catalog()`. Integration must capture one configuration per property
per batch and normalize retained requests before persistence; this API alone does
not clone hits, strip original bodies, validate retained events or enqueue them.
