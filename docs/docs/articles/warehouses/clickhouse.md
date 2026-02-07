# ClickHouse

ClickHouse is a fast, open-source column-oriented database management system. Configuring d8a to use ClickHouse is straightforward and requires no external setup.

## Configuration

:::info Tip
   Full configuration reference is available [here](/articles/config#--clickhouse-database).
:::

Add the following to your `config.yaml` file:

```yaml
warehouse:
  driver: clickhouse
clickhouse:
  host: localhost
  port: "9000"
  database: d8a
  username: default
  password: "your-password"
```

## Important notes

- **Engine Support**: d8a currently supports only the `MergeTree` engine. Tables are created with `ENGINE = MergeTree()`.
- **Distributed/Replicated Setups**: Distributed and Replicated table setups are not supported at the moment. Use a single ClickHouse instance or request this feature in GitHub issues.
- **Nullability**: Nullable columns from the schema in Clickhouse are stored as `NOT NULL` with `DEFAULT`. This avoids [`Nullable(T)` storage overhead](https://clickhouse.com/docs/optimize/avoid-nullable-columns) while preserving semantic nullability. Missing or `nil` values are automatically converted to type-specific defaults (e.g., `''` for strings, `0` for numbers, `'1970-01-01'` for dates).

## Metadata

ClickHouse-specific optimizations can be applied via Arrow field metadata:

| Metadata Key | Value | Description |
|--------------|-------|-------------|
| `clickhouse.low_cardinality` | `"true"` | Wraps the column type as `LowCardinality(T)` for improved storage efficiency and query performance on columns with low cardinality. Applies to any type supported by Clickhouse; |

## Verifying your setup

After configuring ClickHouse, start d8a and check the logs. You should see messages indicating successful connection to ClickHouse.

