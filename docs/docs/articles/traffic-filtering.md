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
      expression: 'starts_with(ip_address, "192.168")'
```

### Inline flags (CLI/environment variables)

```bash
export FILTERS_FIELDS="ip_address"
export FILTERS_CONDITIONS='{"name":"internal_traffic","type":"exclude","test_mode":false,"expression":"starts_with(ip_address, \"192.168\")"}'
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

d8a provides the following built-in functions for filter expressions, in addition to standard expr-lang operators:

### String functions

#### `starts_with(str string, prefix string) bool`
Returns `true` if `str` begins with `prefix`.

```yaml
expression: 'starts_with(ip_address, "192.168")'
```

#### `ends_with(str string, suffix string) bool`
Returns `true` if `str` ends with `suffix`.

```yaml
expression: 'ends_with(ip_address, ".100")'
```

#### `contains(str string, substring string) bool`
Returns `true` if `str` contains `substring`.

```yaml
expression: 'contains(ip_address, "168.1")'
```

#### `matches(str string, pattern string) bool`
Returns `true` if `str` matches the regular expression `pattern`.

```yaml
expression: 'matches(ip_address, "^10\\.0\\.0\\.(1[0-9]|2[0-5])$")'
```

### Network functions

#### `in_cidr(ip string, cidr string) bool`
Returns `true` if IP address `ip` is within CIDR range `cidr`.

```yaml
expression: 'in_cidr(ip_address, "10.0.0.0/8")'
```

### Standard operators

All standard expr-lang operators are available, including:
- Comparison: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `&&` (and), `||` (or), `!` (not)
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Ternary: `condition ? true_value : false_value`

See [expr language definition](https://expr-lang.org/docs/language-definition) for complete operator reference.

## Examples

### Block internal traffic
```yaml
- name: "internal_traffic"
  type: exclude
  test_mode: false
  expression: 'in_cidr(ip_address, "192.168.0.0/16") || in_cidr(ip_address, "10.0.0.0/8")'
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
      expression: 'starts_with(user_id, "test_") && in_cidr(ip_address, "10.0.0.0/8")'
```

## Related configuration

See the [Configuration](./config.md) reference for all available configuration options.
