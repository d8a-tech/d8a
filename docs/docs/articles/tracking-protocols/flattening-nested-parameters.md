# Flattening nested parameters

Flattening nested parameters allows extracting values from protocol-specific repeated structures and writing them into top-level custom columns.

## Configuration

### YAML configuration file

```yaml
protocol:
  ga4_params:
    - name: campaign_id
      type: string

  matomo_custom_dimensions:
    - slot: 3
      name: plan_tier
      scope: event

  matomo_custom_variables:
    - name: ab_test_group
      scope: session
```

### Inline flags (CLI/environment variables)

```bash
export PROTOCOL_GA4_PARAMS='{"name":"campaign_id","type":"string"}'
export PROTOCOL_MATOMO_CUSTOM_DIMENSIONS='{"slot":3,"name":"plan_tier","scope":"event"}'
export PROTOCOL_MATOMO_CUSTOM_VARIABLES='{"name":"ab_test_group","scope":"session"}'
```

Or via CLI:

```bash
./d8a run \
  --protocol-ga4-params '{"name":"campaign_id","type":"string"}' \
  --protocol-matomo-custom-dimensions '{"slot":3,"name":"plan_tier","scope":"event"}'
```

### Precedence rules

When both YAML and inline flags are provided:
- `protocol.ga4_params`: Flag and env values are appended to YAML entries
- `protocol.matomo_custom_dimensions`: Flag and env values are appended to YAML entries
- `protocol.matomo_custom_variables`: Flag and env values are appended to YAML entries

## GA4: Event params shortcuts

Use `protocol.ga4_params` to build event-scoped columns from GA4 `params` entries (`ep.*` and `epn.*`).

Each entry supports:
- **name**: Output column name (required)
- **scope**: `event` (optional; defaults to `event`)
- **type**: `string`, `int64`, or `float64` (optional; defaults to `string`)

Example:

```yaml
protocol:
  ga4_params:
    - name: campaign_id
      type: string
```

## Matomo: Custom dimensions shortcuts

Use `protocol.matomo_custom_dimensions` to build columns from Matomo `dimensionN` values.

Each entry supports:
- **slot**: Dimension slot number (required)
- **name**: Output column name (required)
- **scope**: `event` or `session` (optional; defaults to `event`)
- **type**: `string` (optional; defaults to `string`)

Example:

```yaml
protocol:
  matomo_custom_dimensions:
    - slot: 3
      name: plan_tier
      scope: session
```

## Matomo: Custom variables shortcuts

Use `protocol.matomo_custom_variables` to build columns from Matomo custom variables (`cvar` and `_cvar`).

Each entry supports:
- **name**: Custom variable key to match (required)
- **scope**: `event` or `session` (optional; defaults to `event`)
- **type**: `string` (optional; defaults to `string`)

Example:

```yaml
protocol:
  matomo_custom_variables:
    - name: ab_test_group
      scope: event
```

## Related configuration

See the [Configuration](../config.md) reference for all available configuration options.
