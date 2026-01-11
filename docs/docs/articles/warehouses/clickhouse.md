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
- **Distributed/Replicated Setups**: Distributed and Replicated table setups are not supported at the moment. Use a single ClickHouse instance.

## Verifying your setup

After configuring ClickHouse, start d8a and check the logs. You should see messages indicating successful connection to ClickHouse.

