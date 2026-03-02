# Files

The files warehouse driver writes session data to either local filesystem or object storage (S3/MinIO or GCS). It does not require a running database.

Data is written continuously to an active file per stream. When a file reaches a size or age threshold it is sealed and uploaded. d8a recovers any undelivered segments automatically on restart.

## What you need

- A local spool directory with sufficient disk space for buffering
- One of: a destination directory (local), an S3/MinIO bucket, or a GCS bucket

## Configuration

:::info Tip
Full configuration reference is available [here](/articles/config#--warehouse-files-format).
:::

Add the following to your `config.yaml` file:

```yaml
storage:
  spool_enabled: true        # required by the files warehouse driver
  spool_directory: ./spool   # where active/sealed segments are staged

warehouse:
  driver: files
  files:
    format: csv
    storage: filesystem      # filesystem, s3, or gcs
    filesystem:
      path: /data/warehouse
```

## Storage destinations

### Filesystem

Files are moved to the configured directory once sealed. Useful for local pipelines or when another process picks them up from disk.

```yaml
warehouse:
  driver: files
  files:
    storage: filesystem
    filesystem:
      path: /data/warehouse
```

### S3 / MinIO

```yaml
warehouse:
  driver: files
  files:
    storage: s3
    s3:
      host: s3.amazonaws.com
      bucket: my-bucket
      access_key: AKIAIOSFODNN7EXAMPLE
      secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      region: us-east-1
      protocol: https
```

### GCS

```yaml
warehouse:
  driver: files
  files:
    storage: gcs
    gcs:
      bucket: my-gcs-bucket
      creds_json: |
        { "type": "service_account", ... }
```

Leave `creds_json` empty to use Application Default Credentials (ADC).

## Segment tuning

Segments are sealed when either threshold is crossed first.

| Option | Default | Description |
|---|---|---|
| `warehouse.files.max_segment_size` | `1073741824` (1 GiB) | Seal when the active file reaches this size |
| `warehouse.files.max_segment_age` | `1h` | Seal when the active file is this old |
| `warehouse.files.seal_check_interval` | `15s` | How often to evaluate sealing triggers |

## Path template

The files warehouse writes data to paths generated from a configurable template. See [`--warehouse-files-path-template`](/articles/config/#--warehouse-files-path-template) for the default value and customization options.

**Available variables:**

| Variable | Type | Description |
|---|---|---|
| `Table` | string | Escaped table name |
| `Schema` | string | 16-character schema fingerprint |
| `SegmentID` | string | Segment identifier (unixSeconds_uuid) |
| `Extension` | string | File extension (csv or csv.gz) |
| `Year` | int | Year (e.g., 2026) |
| `Month` | int | Month number (1-12) |
| `MonthPadded` | string | Month with leading zero (01-12) |
| `Day` | int | Day of month (1-31) |
| `DayPadded` | string | Day with leading zero (01-31) |

**Example:** `table={{.Table}}/year={{.Year}}/month={{.MonthPadded}}/day={{.DayPadded}}/{{.SegmentID}}.{{.Extension}}`

## Important notes

- **Spool required**: `storage.spool_enabled` must be `true`. The files warehouse uses the spool directory to stage segments before upload.
- **Schema migrations**: `CreateTable` and `AddColumn` are no-ops. The files warehouse does not create or alter tables. Schema evolution is handled by the consumer of the files.
- **Crash recovery**: On startup d8a scans the spool directory, moves any interrupted uploads back to sealed, and retries them.
- **Upload retries**: A segment that fails to upload is retried up to 3 times. After 3 failures it is moved to a quarantine directory (`streams/<table>/<fingerprint>/failed/`) and will not be retried until manually addressed.

## Verifying your setup

After configuring the files warehouse, start d8a and check the logs. You should see messages indicating segments being sealed and uploaded to your configured destination.

## Querying your files

The files written by this driver can be consumed directly by most analytics warehouses without an external pipeline.

- **Snowflake** — Automate ingestion using Snowpipe to continuously load new CSV/CSV.GZ files from an external stage into a target table as soon as they arrive. This completely eliminates the need for an external pipeline, though querying a configured External Table with auto-refresh is an alternative if you prefer zero data movement. See the [Snowflake Snowpipe documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro.html) to set this up.

- **Amazon Redshift** — Use Redshift Spectrum to create an external table and query the files directly from S3, leveraging partition pruning if you update your prefixes to the standard `year=YYYY/month=MM/day=DD` format. While Spectrum requires no pipeline for direct querying, Redshift's native auto-copy feature can automatically ingest new files into managed storage without external orchestrators. Check out the [Redshift Spectrum documentation](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html) for implementation steps.

- **Databricks SQL** — Use Auto Loader to incrementally and automatically process new CSV files as they land in your cloud storage. This serves as a native, pipeline-free ingestion method into Delta tables, but renaming your prefixes to standard Hive partitioning will significantly optimize the initial directory discovery. Read the [Databricks Auto Loader documentation](https://docs.databricks.com/ingestion/auto-loader/index.html) for configuration.

- **Azure Synapse Analytics** — Query object storage directly using Serverless SQL pools by creating an external table or using the `OPENROWSET` function on your bucket path. This requires no pipeline for basic querying, though for optimal performance on large datasets you would need Azure Data Factory/Synapse Pipelines to copy the data into Dedicated SQL pools. Explore the [Azure Synapse Serverless SQL documentation](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/query-data-storage) to get started.

- **Starburst / Trino** — Connect a Hive or Iceberg catalog to your bucket and define an external table directly over your CSV directory. Trino is a federated query engine, so it inherently requires no ingestion pipeline to query the files in-place, but changing your directory structure to `y=YYYY/m=MM/d=DD` is highly recommended for efficient partition pruning. Visit the [Trino Hive Connector documentation](https://trino.io/docs/current/connector/hive.html) for setup details.

- **Apache Druid** — Continuously ingest the CSVs using Druid's native batch or streaming ingestion by pointing a supervisor spec to the object storage path. No external pipeline is needed because Druid manages the ingestion tasks internally, but you must ensure your CSV contains a timestamp column for Druid's mandatory primary time partitioning. Review the [Apache Druid Ingestion documentation](https://druid.apache.org/docs/latest/ingestion/index.html) to configure your spec.

- **Firebolt** — Create an external table pointing to your bucket and then use standard `INSERT INTO ... SELECT` statements to load data into a fact table. While external tables allow direct querying without a pipeline, moving data to native Firebolt tables requires some external orchestration (such as Airflow) to regularly trigger the ingestion of new files. More information is available in the [Firebolt External Tables documentation](https://docs.firebolt.io/sql-reference/commands/data-management/external-table.html).

- **Teradata Vantage** — Use the Native Object Store (NOS) feature to create a foreign table directly over your CSV/CSV.GZ files in cloud storage. This eliminates the need for an external pipeline for direct querying, though using standard Hive naming conventions (`$path/$var=...`) will allow NOS to filter partitions efficiently without scanning every file. The [Teradata Native Object Store documentation](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Foreign-Tables) outlines the syntax required.

- **Google BigQuery** — For the best experience, use the dedicated [BigQuery warehouse driver](/articles/warehouses/bigquery) instead. If you prefer to consume the files directly, you can query them by creating an external table over the bucket, ideally updating your prefix to Hive partitioning (`y=YYYY/m=MM/d=DD`) for better query performance. This works entirely without a pipeline, but for faster querying on frequent appends you should use native scheduled queries to move data into partitioned native tables. Refer to the [BigQuery External Tables documentation](https://cloud.google.com/bigquery/docs/external-tables) for more details.

- **ClickHouse** — For the best experience, use the dedicated [ClickHouse warehouse driver](/articles/warehouses/clickhouse) instead. If you prefer to consume the files directly, you can use the `S3` table engine or function to query them using glob patterns like `y/m/d/*.csv.gz`. A pipeline isn't strictly necessary as you can set up a Materialized View over the S3 engine to automatically ingest new data into a native MergeTree table, but adding the `y=YYYY/` Hive format improves partition filtering. Learn more in the [ClickHouse S3 Engine documentation](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3).

- **DuckDB** — Query the files directly using `read_csv` or the `read_csv_auto` function with glob patterns (e.g., `SELECT * FROM read_csv_auto('/data/warehouse/events/**/*.csv')`). DuckDB requires no pipeline and no server — it runs in-process, making it ideal for ad-hoc analysis or lightweight local setups. For S3-hosted files, install the `httpfs` extension and configure your credentials with `SET s3_region`, `SET s3_access_key_id`, and `SET s3_secret_access_key` before querying. See the [DuckDB CSV import documentation](https://duckdb.org/docs/data/csv/overview.html) for details.
