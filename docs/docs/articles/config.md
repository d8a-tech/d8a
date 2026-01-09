# Configuration reference

## Introduction

This document describes all available configuration options for d8a.

When multiple configuration sources are provided, values are resolved in the following order of precedence:

- CLI flags (highest priority)
- Environment variables
- YAML configuration file (lowest priority)

The configuration file is a YAML file. You can specify a custom location using the `--config` or `-c` flag.

## Configuration keys
---

### --bigquery-creds-json

BigQuery service account JSON (raw or base64). Only applicable when warehouse-driver is set to 'bigquery'.

**Configuration key:** `bigquery.creds_json`  
**Environment variable:** `BIGQUERY_CREDS_JSON`

---

### --bigquery-dataset-name

BigQuery dataset name. Only applicable when warehouse-driver is set to 'bigquery'.

**Configuration key:** `bigquery.dataset_name`  
**Environment variable:** `BIGQUERY_DATASET_NAME`

---

### --bigquery-project-id

BigQuery GCP project ID. Only applicable when warehouse-driver is set to 'bigquery'.

**Configuration key:** `bigquery.project_id`  
**Environment variable:** `BIGQUERY_PROJECT_ID`

---

### --bigquery-query-timeout

BigQuery query timeout. Only applicable when warehouse-driver is set to 'bigquery'.

**Configuration key:** `bigquery.query_timeout`  
**Environment variable:** `BIGQUERY_QUERY_TIMEOUT`

**Default:** `30s`

---

### --bigquery-table-creation-timeout

BigQuery table creation timeout. Only applicable when warehouse-driver is set to 'bigquery'.

**Configuration key:** `bigquery.table_creation_timeout`  
**Environment variable:** `BIGQUERY_TABLE_CREATION_TIMEOUT`

**Default:** `10s`

---

### --bigquery-writer-type

BigQuery writer type (loadjob or streaming). Only applicable when warehouse-driver is set to 'bigquery'.

**Configuration key:** `bigquery.writer_type`  
**Environment variable:** `BIGQUERY_WRITER_TYPE`

**Default:** `loadjob`

---

### --clickhouse-database

ClickHouse database name. Only applicable when warehouse-driver is set to 'clickhouse'.

**Configuration key:** `clickhouse.database`  
**Environment variable:** `WAREHOUSE_CLICKHOUSE_DB`

---

### --clickhouse-host

ClickHouse host. Only applicable when warehouse-driver is set to 'clickhouse'.

**Configuration key:** `clickhouse.host`  
**Environment variable:** `WAREHOUSE_CLICKHOUSE_HOST`

---

### --clickhouse-password

ClickHouse password. Only applicable when warehouse-driver is set to 'clickhouse'.

**Configuration key:** `clickhouse.password`  
**Environment variable:** `WAREHOUSE_CLICKHOUSE_PASSWORD`

---

### --clickhouse-port

ClickHouse port. Only applicable when warehouse-driver is set to 'clickhouse'.

**Configuration key:** `clickhouse.port`  
**Environment variable:** `WAREHOUSE_CLICKHOUSE_PORT`

**Default:** `9000`

---

### --clickhouse-username

ClickHouse username. Only applicable when warehouse-driver is set to 'clickhouse'.

**Configuration key:** `clickhouse.username`  
**Environment variable:** `WAREHOUSE_CLICKHOUSE_USER`

---

### --dbip-destination-directory

Directory where the DB-IP database files are stored after downloading from the OCI registry. If the database already exists at this location, the download is skipped. Defaults to a temporary directory if not specified.

**Configuration key:** `dbip.destination_directory`  
**Environment variable:** `DBIP_DESTINATION_DIRECTORY`

**Default:** `/tmp/dbip`

---

### --dbip-download-timeout

Maximum time to wait for downloading the DB-IP MaxMind database from the OCI registry during program startup. If the download exceeds this timeout, the program will fail to start with DBIP columns enabled.

**Configuration key:** `dbip.download_timeout`  
**Environment variable:** `DBIP_DOWNLOAD_TIMEOUT`

**Default:** `1m0s`

---

### --dbip-enabled

When enabled, adds geolocation column implementations (city, country, etc.) using DB-IP database. On program startup, downloads the DB-IP database from the OCI registry (ghcr.io/d8a-tech). The database is cached locally and reused on subsequent runs if already present.

**Configuration key:** `dbip.enabled`  
**Environment variable:** `DBIP_ENABLED`

---

### --monitoring-enabled

Enable OpenTelemetry metrics

**Configuration key:** `monitoring.enabled`  
**Environment variable:** `MONITORING_ENABLED`

---

### --monitoring-otel-endpoint

OTel collector endpoint for metrics

**Configuration key:** `monitoring.otel_endpoint`  
**Environment variable:** `MONITORING_OTEL_ENDPOINT`

**Default:** `localhost:4317`

---

### --monitoring-otel-export-interval

Interval for exporting metrics to OTel collector

**Configuration key:** `monitoring.otel_export_interval`  
**Environment variable:** `MONITORING_OTEL_EXPORT_INTERVAL`

**Default:** `30s`

---

### --monitoring-otel-insecure

Allow insecure (non-TLS) connection to OTel collector

**Configuration key:** `monitoring.otel_insecure`  
**Environment variable:** `MONITORING_OTEL_INSECURE`

---

### --property-id

Property ID, used to satisfy interfaces required by d8a cloud. Ends up as column in the warehouse.

**Configuration key:** `property.id`  
**Environment variable:** `PROPERTY_ID`

**Default:** `default`

---

### --property-name

Property name, used to satisfy interfaces required by d8a cloud. Ends up as column in the warehouse.

**Configuration key:** `property.name`  
**Environment variable:** `PROPERTY_NAME`

**Default:** `Default property`

---

### --property-settings-split-by-campaign

When enabled, splits a session into multiple sessions when the UTM campaign parameter value changes between events. This allows tracking separate sessions for different marketing campaigns within the same user visit.

**Configuration key:** `property.settings.split_by_campaign`  
**Environment variable:** `PROPERTY_SETTINGS_SPLIT_BY_CAMPAIGN`

**Default:** `true`

---

### --property-settings-split-by-max-events

Splits a session into multiple sessions when the number of events exceeds this value. This prevents sessions with excessive event counts from being stored as a single large session.

**Configuration key:** `property.settings.split_by_max_events`  
**Environment variable:** `PROPERTY_SETTINGS_SPLIT_BY_MAX_EVENTS`

**Default:** `1000`

---

### --property-settings-split-by-time-since-first-event

Splits a session into multiple sessions when the time elapsed since the first event exceeds this duration. This prevents extremely long sessions from being grouped together, creating more meaningful session boundaries.

**Configuration key:** `property.settings.split_by_time_since_first_event`  
**Environment variable:** `PROPERTY_SETTINGS_SPLIT_BY_TIME_SINCE_FIRST_EVENT`

**Default:** `12h0m0s`

---

### --property-settings-split-by-user-id

When enabled, splits a session into multiple sessions when the user ID value changes between events. This ensures that events from different authenticated users are not grouped into the same session.

**Configuration key:** `property.settings.split_by_user_id`  
**Environment variable:** `PROPERTY_SETTINGS_SPLIT_BY_USER_ID`

**Default:** `true`

---

### --protocol

Protocol to use for tracking requests. Valid values are 'ga4'.

**Configuration key:** `protocol`  
**Environment variable:** `PROTOCOL`

**Default:** `ga4`

---

### --receiver-batch-size

Maximum number of hits to accumulate before flushing to the queue storage. When this many hits are received, they are immediately flushed even if the timeout hasn't been reached.

**Configuration key:** `receiver.batch_size`  
**Environment variable:** `RECEIVER_BATCH_SIZE`

**Default:** `5000`

---

### --receiver-batch-timeout

Maximum time to wait before flushing accumulated hits to the queue storage. Hits are flushed when either this timeout is reached or the batch size limit is exceeded, whichever comes first.

**Configuration key:** `receiver.batch_timeout`  
**Environment variable:** `RECEIVER_BATCH_TIMEOUT`

**Default:** `1s`

---

### --receiver-max-hit-kbytes

Maximum size of a hit in kilobytes. Tracking requests are rejected if they contain a hit, which exceeds this size.

**Configuration key:** `receiver.max_hit_kbytes`  
**Environment variable:** `RECEIVER_MAX_HIT_KBYTES`

**Default:** `128`

---

### --server-port

Port to listen on for HTTP server

**Configuration key:** `server.port`  
**Environment variable:** `SERVER_PORT`

**Default:** `8080`

---

### --sessions-duration

Maximum time period of inactivity after which a proto-session is considered expired and ready to be closed. The system uses a timing wheel to schedule session closures based on each hit's server received time plus this duration. After this period elapses without new hits, the proto-session is finalized and written to the warehouse as a completed session.

**Configuration key:** `sessions.duration`  
**Environment variable:** `SESSIONS_DURATION`

**Default:** `30m0s`

---

### --sessions-join-by-session-stamp

When enabled, the system will merge proto-sessions that share the same session stamp identifier, even if they have different client IDs. This allows tracking user sessions across different devices or browsers when they share a common session identifier, enabling cross-device session continuity for authenticated or identified users.

**Configuration key:** `sessions.join_by_session_stamp`  
**Environment variable:** `SESSIONS_JOIN_BY_SESSION_STAMP`

**Default:** `true`

---

### --sessions-join-by-user-id

When enabled, the system will merge proto-sessions that share the same user ID, even if they have different client IDs. This enables cross-device session tracking for authenticated users, allowing hits from different devices or browsers to be grouped into a single session when they share the same authenticated user identifier. Only hits that include a user ID value will participate in this joining behavior.

**Configuration key:** `sessions.join_by_user_id`  
**Environment variable:** `SESSIONS_JOIN_BY_USER_ID`

---

### --storage-bolt-directory

Directory path where BoltDB database files are stored. This directory hosts two databases: 'bolt.db' for proto-session data, identifier metadata, and timing wheel bucket information, and 'bolt_kv.db' for key-value storage. These databases persist session state across restarts and are essential for session management functionality.

**Configuration key:** `storage.bolt_directory`  
**Environment variable:** `STORAGE_BOLT_DIRECTORY`

**Default:** `.`

---

### --storage-queue-directory

Directory path where batched hits are stored in a filesystem-based queue before being processed by background workers. This directory acts as a persistent buffer between the receiver and the session processing pipeline.

**Configuration key:** `storage.queue_directory`  
**Environment variable:** `STORAGE_QUEUE_DIRECTORY`

**Default:** `./queue`

---

### --warehouse-driver

Target warehouse driver (clickhouse, bigquery, console, or noop)

**Configuration key:** `warehouse.driver`  
**Environment variable:** `WAREHOUSE_DRIVER`

**Default:** `console`

---

### --warehouse-table

Target warehouse table name.

**Configuration key:** `warehouse.table`  
**Environment variable:** `WAREHOUSE_TABLE`

**Default:** `events`

---

