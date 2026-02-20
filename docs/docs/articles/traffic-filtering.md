# Traffic filtering

Traffic filtering allows excluding or allowing tracking events based on conditions evaluated during hit processing before warehouse writes.

## Filter condition structure

Each filter condition consists of:
- **name**: String identifier
- **type**: `exclude` (block matching traffic) or `allow` (permit only matching traffic)
- **test_mode**: When `true`, sets `traffic_filter_name` column value without including or excluding events
- **expression**: Expression evaluated against available fields

When any `allow` filter exists, only traffic matching at least one allow condition is processed. Exclude filters block matching traffic.

## Configuration

### YAML configuration file

```yaml
filters:
  fields:
    - ip_address

  conditions:
    - name: "internal_traffic"
      type: exclude
      test_mode: false
      expression: 'ip_address startsWith "192.168"'
```

### Inline flags (CLI/environment variables)

```bash
export FILTERS_FIELDS="ip_address"
export FILTERS_CONDITIONS='{"name":"internal_traffic","type":"exclude","test_mode":false,"expression":"ip_address startsWith \"192.168\""}'
```

Or via CLI:
```bash
./tracker-api run --filters-conditions '{"name":"internal_traffic","type":"exclude","test_mode":false,"expression":"ip_address == \"10.0.0.1\""}'
```

### Precedence rules

When both YAML and inline flags are provided:
- `filters.fields`: Flag value replaces YAML value
- `filters.conditions`: Flag values are appended to YAML conditions

## Available fields

The `filters.fields` configuration specifies which event columns are injected into the expression environment.

## Expression interpreter

Filter expressions are evaluated using [expr-lang](https://expr-lang.org/), a fast expression language for Go. See the [expr language definition](https://expr-lang.org/docs/language-definition) for complete syntax reference.

## API reference

d8a provides the following custom function for filter expressions. All standard expr-lang operators and string functions are also available — see [expr-lang string functions](https://expr-lang.org/docs/language-definition#string-functions) for the full reference.

### Network functions

#### `inCidr(ip string, cidr string) bool`
Returns `true` if IP address `ip` is within CIDR range `cidr`. Supports both IPv4 and IPv6.

```yaml
expression: 'inCidr(ip_address, "10.0.0.0/8")'
```

### Standard expr operators

All standard expr-lang operators are available, including:
- Comparison: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `&&` (and), `||` (or), `!` (not)
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Ternary: `condition ? true_value : false_value`
- String: `contains`, `startsWith`, `endsWith`, `matches` (regex)

Examples:

```yaml
expression: 'ip_address startsWith "192.168"'
expression: 'hostname endsWith ".internal"'
expression: 'ip_address matches "^10\\.0\\.0\\.[0-9]{1,3}$"'
expression: 'user_agent contains "bot"'
```

See [expr-lang language definition](https://expr-lang.org/docs/language-definition) for complete syntax and function reference.

## Examples

### Block internal traffic
```yaml
- name: "internal_traffic"
  type: exclude
  test_mode: false
  expression: 'inCidr(ip_address, "192.168.0.0/16") || inCidr(ip_address, "10.0.0.0/8")'
```

### Test mode for office traffic
```yaml
- name: "office_traffic"
  type: exclude
  test_mode: true  # Sets traffic_filter_name without excluding
  expression: 'ip_address == "203.0.113.50"'
```

### Allow only specific events
```yaml
- name: "page_views_only"
  type: allow
  test_mode: false
  expression: 'event_name == "page_view" || event_name == "screen_view"' # requires event_name in filters.fields
```

### Complex condition with multiple fields
```yaml
filters:
  fields:
    - ip_address
    - user_id

  conditions:
    - name: "internal_test_users"
      type: exclude
      test_mode: false
      expression: 'user_id startsWith "test_" && inCidr(ip_address, "10.0.0.0/8")'
```

## Related configuration

See the [Configuration](./config.md) reference for all available configuration options.
