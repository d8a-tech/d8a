# Traffic filtering

This article describes how to configure traffic filtering in d8a to exclude or allow specific traffic based on custom conditions.

## Overview

Traffic filtering allows you to exclude or allow tracking events based on conditions evaluated against event fields. Filters are evaluated during hit processing before events are written to the warehouse.

Each filter condition has:
- **name**: String identifier for the filter
- **type**: Either `exclude` (block matching traffic) or `allow` (permit only matching traffic)
- **test_mode**: When `true`, sets metadata (`traffic_filter_name`) without excluding events
- **expression**: Filter expression using available fields

## Configuration modes

d8a supports two modes for configuring traffic filters: YAML configuration file and inline flags (CLI/environment variables). Both modes can be used together, with inline flag values appended to YAML configuration.

### YAML configuration

Define filters in your configuration file under the `filters` section:

```yaml
filters:
  fields:
    - ip_address
    - event_name
    - user_id

  conditions:
    - name: "internal_traffic"
      type: exclude
      test_mode: false
      expression: 'starts_with(ip_address, "192.168")'

    - name: "vpn_only"
      type: allow
      test_mode: false
      expression: 'in_cidr(ip_address, "100.64.0.0/10")'
```

### Inline flags

Pass filters as command-line flags or environment variables using JSON-encoded strings:

**Environment variable:**
```bash
export FILTERS_FIELDS="ip_address,event_name"
export FILTERS_CONDITIONS='{"name":"internal_traffic","type":"exclude","test_mode":false,"expression":"starts_with(ip_address, \"192.168\")"}'
```

**CLI flag:**
```bash
./tracker-api run \
  --filters-fields ip_address \
  --filters-fields event_name \
  --filters-conditions '{"name":"internal_traffic","type":"exclude","test_mode":false,"expression":"starts_with(ip_address, \"192.168\")"}'
```

**Combined usage:**
When both YAML and inline flags are provided:
- `filters.fields`: Flag value **replaces** YAML value
- `filters.conditions`: Flag values are **appended** to YAML conditions

## Available fields

The `filters.fields` configuration specifies which event columns are available in filter expressions. Only fields listed here can be referenced in condition expressions.

Default: `["ip_address"]`

## Filter types

### Exclude filters

`type: exclude` blocks traffic matching the expression. Excluded events are not written to the warehouse.

### Allow filters

`type: allow` permits only traffic matching the expression. Events not matching any allow filter are blocked.

**Important:** When multiple conditions are present, allow filters take precedence. If any allow filter exists, only traffic matching at least one allow condition is processed.

### Test mode

Set `test_mode: true` to evaluate the filter without excluding events. Matching events receive a `traffic_filter_name` metadata field with the filter name, allowing analysis without data loss.

## Expression syntax

Filter expressions use the expr language with string comparison and network functions:

**String operators:**
- `starts_with(field, "prefix")` - Check string prefix
- `ends_with(field, "suffix")` - Check string suffix
- `contains(field, "substring")` - Check substring presence
- `matches(field, "regex")` - Regular expression match
- `field == "value"` - Exact equality

**Network operators:**
- `in_cidr(ip_address, "10.0.0.0/8")` - CIDR range check

**Logical operators:**
- `&&` (and), `||` (or), `!` (not)

## Examples

**Block internal traffic:**
```yaml
- name: "internal_traffic"
  type: exclude
  test_mode: false
  expression: 'in_cidr(ip_address, "192.168.0.0/16")'
```

**Test mode for office traffic:**
```yaml
- name: "office_traffic"
  type: exclude
  test_mode: true
  expression: 'ip_address == "203.0.113.50"'
```

**Allow only specific events:**
```yaml
- name: "page_views_only"
  type: allow
  test_mode: false
  expression: 'event_name == "page_view"'
```

## Related configuration

See the [Configuration](./config.md) reference for all available configuration options.
