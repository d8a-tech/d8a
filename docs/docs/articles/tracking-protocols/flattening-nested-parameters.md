# Flattening nested parameters

Flattening nested parameters allows extracting values from protocol-specific repeated structures and writing them into top-level custom columns.

## Minimal example configuration

### YAML configuration file

```yaml
ga4:
  params:
    - name: campaign_id
matomo:
  custom_dimensions:
    - slot: 3
      name: plan_tier
  custom_variables:
    - name: ab_test_group
```

## GA4: Event params

Use `ga4.params` to build event-scoped columns from GA4 `params` entries (`ep.*` and `epn.*`).

Each entry supports:
- **name**: Parameter key to match (required)
- **column_name**: Output column name override (optional; defaults to `params_<name>`). If set, this value is used as the output column name.
- **type**: `string`, `int64`, or `float64` (optional; defaults to `string`). This controls value casting and source value field selection.

Example:

```yaml
ga4:
  params:
    - name: campaign_id
      type: string
```

## Matomo: Custom dimensions

Use `matomo.custom_dimensions` to build columns from Matomo `dimensionN` values.

Each entry supports:
- **slot**: Dimension slot number (required)
- **name**: Dimension name label (required)
- **column_name**: Output column name override (optional; defaults to `custom_dimension_<name>`). If set, this value is used as the output column name.
- **scope**: `event` or `session` (optional; defaults to `event`). This controls whether the value is read per event or resolved on session scope.

Example:

```yaml
matomo:
  custom_dimensions:
    - slot: 3
      name: plan_tier
      scope: session
```

## Matomo: Custom variables

Use `matomo.custom_variables` to build columns from Matomo custom variables (`cvar` and `_cvar`).

Each entry supports:
- **name**: Custom variable key to match (required)
- **column_name**: Output column name override (optional; defaults to `custom_variable_<name>`). If set, this value is used as the output column name.
- **scope**: `event` or `session` (optional; defaults to `event`). This controls whether the value is read per event or resolved on session scope.

Example:

```yaml
matomo:
  custom_variables:
    - name: ab_test_group
      scope: event
```

## Other configuration options

You can configure the same settings also using cli flags or environment variables.

### Inline flags (CLI/environment variables)

```bash
export GA4_PARAMS='[{"name":"campaign_id","type":"string"}]'
export MATOMO_CUSTOM_DIMENSIONS='[{"slot":3,"name":"plan_tier","column_name":"plan_tier_custom","scope":"event"}]'
export MATOMO_CUSTOM_VARIABLES='[{"name":"ab_test_group","column_name":"ab_group_custom","scope":"session"}]'
```

Or via CLI:

```bash
./d8a run \
  --ga4-params '[{"name":"campaign_id","type":"string"}]' \
  --matomo-custom-dimensions '[{"slot":3,"name":"plan_tier","scope":"event"}]'
```

### Precedence rules

When both YAML and inline flags are provided:
- `ga4.params`: Flag and env values are appended to YAML entries
- `matomo.custom_dimensions`: Flag and env values are appended to YAML entries
- `matomo.custom_variables`: Flag and env values are appended to YAML entries

## Related configuration

See the [Configuration](../config.md) reference for all available configuration options.
