# Flattening nested parameters

Flattening nested parameters allows extracting values from protocol-specific repeated structures and writing them into top-level custom columns.

## Configuration

### YAML configuration file

```yaml
ga4:
  params:
    - name: campaign_id
      type: string

matomo:
  custom_dimensions:
    - slot: 3
      name: plan_tier
      column_name: plan_tier_custom
      scope: event

  custom_variables:
    - name: ab_test_group
      column_name: ab_group_custom
      scope: session
```

### Inline flags (CLI/environment variables)

```bash
export GA4_PARAMS='{"name":"campaign_id","type":"string"}'
export MATOMO_CUSTOM_DIMENSIONS='{"slot":3,"name":"plan_tier","column_name":"plan_tier_custom","scope":"event"}'
export MATOMO_CUSTOM_VARIABLES='{"name":"ab_test_group","column_name":"ab_group_custom","scope":"session"}'
```

Or via CLI:

```bash
./d8a run \
  --ga4-params '{"name":"campaign_id","type":"string"}' \
  --matomo-custom-dimensions '{"slot":3,"name":"plan_tier","scope":"event"}'
```

### Precedence rules

When both YAML and inline flags are provided:
- `ga4.params`: Flag and env values are appended to YAML entries
- `matomo.custom_dimensions`: Flag and env values are appended to YAML entries
- `matomo.custom_variables`: Flag and env values are appended to YAML entries

## GA4: Event params shortcuts

Use `ga4.params` to build event-scoped columns from GA4 `params` entries (`ep.*` and `epn.*`).

Each entry supports:
- **name**: Parameter key to match (required)
- **column_name**: Output column name override (optional)
- **scope**: `event` (optional; defaults to `event`)
- **type**: `string`, `int64`, or `float64` (optional; defaults to `string`)

Default output name: `params_<name>`

Example:

```yaml
ga4:
  params:
    - name: campaign_id
      type: string
      # output column: params_campaign_id
```

## Matomo: Custom dimensions shortcuts

Use `matomo.custom_dimensions` to build columns from Matomo `dimensionN` values.

Each entry supports:
- **slot**: Dimension slot number (required)
- **name**: Dimension name label (required)
- **column_name**: Output column name override (optional)
- **scope**: `event` or `session` (optional; defaults to `event`)
- **type**: `string` (optional; defaults to `string`)

Default output name: `custom_dimension_<name>`

Example:

```yaml
matomo:
  custom_dimensions:
    - slot: 3
      name: plan_tier
      scope: session
      # output column: custom_dimension_plan_tier
```

## Matomo: Custom variables shortcuts

Use `matomo.custom_variables` to build columns from Matomo custom variables (`cvar` and `_cvar`).

Each entry supports:
- **name**: Custom variable key to match (required)
- **column_name**: Output column name override (optional)
- **scope**: `event` or `session` (optional; defaults to `event`)
- **type**: `string` (optional; defaults to `string`)

Default output name: `custom_variable_<name>`

Example:

```yaml
matomo:
  custom_variables:
    - name: ab_test_group
      scope: event
      # output column: custom_variable_ab_test_group
```

## Related configuration

See the [Configuration](../config.md) reference for all available configuration options.
