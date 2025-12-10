package cmd

import (
	"os"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v3"
)

var debugFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "debug",
	Usage:   "Enable debug mode",
	Sources: defaultSourceChain("DEBUG", "debug"),
	Value:   false,
}

var traceFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "trace",
	Usage:   "Enable trace mode",
	Sources: defaultSourceChain("TRACE", "trace"),
	Value:   false,
}

var serverPortFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "server-port",
	Usage:   "Port to listen on for HTTP server",
	Sources: defaultSourceChain("SERVER_PORT", "server.port"),
	Value:   8080,
}

var receiverBatchSizeFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "receiver-batch-size",
	Usage:   "Maximum number of hits to accumulate before flushing to the queue storage. When this many hits are received, they are immediately flushed even if the timeout hasn't been reached.", //nolint:lll // it's a description
	Sources: defaultSourceChain("RECEIVER_BATCH_SIZE", "receiver.batch_size"),
	Value:   5000,
}

var receiverBatchTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "receiver-batch-timeout",
	Usage:   "Maximum time to wait before flushing accumulated hits to the queue storage. Hits are flushed when either this timeout is reached or the batch size limit is exceeded, whichever comes first.", //nolint:lll // it's a description
	Sources: defaultSourceChain("RECEIVER_BATCH_TIMEOUT", "receiver.batch_timeout"),
	Value:   1 * time.Second,
}

var receiverMaxHitKbytesFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "receiver-max-hit-kbytes",
	Usage:   "Maximum size of a hit in kilobytes. Tracking requests are rejected if they contain a hit, which exceeds this size.", //nolint:lll // it's a description
	Sources: defaultSourceChain("RECEIVER_MAX_HIT_KBYTES", "receiver.max_hit_kbytes"),
	Value:   128,
}

var sessionsDurationFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "sessions-duration",
	Usage:   "Maximum time period of inactivity after which a proto-session is considered expired and ready to be closed. The system uses a timing wheel to schedule session closures based on each hit's server received time plus this duration. After this period elapses without new hits, the proto-session is finalized and written to the warehouse as a completed session.", //nolint:lll // it's a description
	Sources: defaultSourceChain("SESSIONS_DURATION", "sessions.duration"),
	Value:   30 * time.Minute,
}

var sessionsJoinBySessionStampFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "sessions-join-by-session-stamp",
	Usage:   "When enabled, the system will merge proto-sessions that share the same session stamp identifier, even if they have different client IDs. This allows tracking user sessions across different devices or browsers when they share a common session identifier, enabling cross-device session continuity for authenticated or identified users.", //nolint:lll // it's a description
	Sources: defaultSourceChain("SESSIONS_JOIN_BY_SESSION_STAMP", "sessions.join_by_session_stamp"),
	Value:   true,
}

var sessionsJoinByUserIDFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "sessions-join-by-user-id",
	Usage:   "When enabled, the system will merge proto-sessions that share the same user ID, even if they have different client IDs. This enables cross-device session tracking for authenticated users, allowing hits from different devices or browsers to be grouped into a single session when they share the same authenticated user identifier. Only hits that include a user ID value will participate in this joining behavior.", //nolint:lll // it's a description
	Sources: defaultSourceChain("SESSIONS_JOIN_BY_USER_ID", "sessions.join_by_user_id"),
	Value:   false,
}

var dbipEnabled *cli.BoolFlag = &cli.BoolFlag{
	Name:    "dbip-enabled",
	Usage:   "When enabled, adds geolocation column implementations (city, country, etc.) using DB-IP database. On program startup, downloads the DB-IP database from the OCI registry (ghcr.io/d8a-tech). The database is cached locally and reused on subsequent runs if already present.", //nolint:lll // it's a description
	Sources: defaultSourceChain("DBIP_ENABLED", "dbip.enabled"),
	Value:   false,
}

var dbipDestinationDirectory *cli.StringFlag = &cli.StringFlag{
	Name:    "dbip-destination-directory",
	Usage:   "Directory where the DB-IP database files are stored after downloading from the OCI registry. If the database already exists at this location, the download is skipped. Defaults to a temporary directory if not specified.", //nolint:lll // it's a description
	Sources: defaultSourceChain("DBIP_DESTINATION_DIRECTORY", "dbip.destination_directory"),
	Value:   filepath.Join(os.TempDir(), "dbip"),
}

var dbipDownloadTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "dbip-download-timeout",
	Usage:   "Maximum time to wait for downloading the DB-IP MaxMind database from the OCI registry during program startup. If the download exceeds this timeout, the program will fail to start with DBIP columns enabled.", //nolint:lll // it's a description
	Sources: defaultSourceChain("DBIP_DOWNLOAD_TIMEOUT", "dbip.download_timeout"),
	Value:   60 * time.Second,
}

var warehouseDriverFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-driver",
	Usage:   "Target warehouse driver (clickhouse, bigquery, console, or noop)",
	Sources: defaultSourceChain("WAREHOUSE_DRIVER", "warehouse.driver"),
	Value:   "console",
}

var warehouseTableFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-table",
	Usage:   "Target warehouse table name.",
	Sources: defaultSourceChain("WAREHOUSE_TABLE", "warehouse.table"),
	Value:   "events",
}

var clickhouseHostFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-host",
	Usage:   "ClickHouse host. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_HOST", "clickhouse.host"),
}

var clickhousePortFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-port",
	Usage:   "ClickHouse port. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PORT", "clickhouse.port"),
	Value:   "9000",
}

var clickhouseDatabaseFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-database",
	Usage:   "ClickHouse database name. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_DB", "clickhouse.database"),
}

var clickhouseUsernameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-username",
	Usage:   "ClickHouse username. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_USER", "clickhouse.username"),
	Value:   "",
}

var clickhousePasswordFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-password",
	Usage:   "ClickHouse password. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PASSWORD", "clickhouse.password"),
	Value:   "",
}

// BigQuery flags
var bigQueryProjectIDFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-project-id",
	Usage:   "BigQuery GCP project ID. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("BIGQUERY_PROJECT_ID", "bigquery.project_id"),
}

var bigQueryDatasetNameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-dataset-name",
	Usage:   "BigQuery dataset name. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("BIGQUERY_DATASET_NAME", "bigquery.dataset_name"),
}

var bigQueryCredsJSONFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-creds-json",
	Usage:   "BigQuery service account JSON (raw or base64). Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("BIGQUERY_CREDS_JSON", "bigquery.creds_json"),
}

var bigQueryWriterTypeFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-writer-type",
	Usage:   "BigQuery writer type (loadjob or streaming). Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("BIGQUERY_WRITER_TYPE", "bigquery.writer_type"),
	Value:   "loadjob",
}

var bigQueryQueryTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "bigquery-query-timeout",
	Usage:   "BigQuery query timeout. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("BIGQUERY_QUERY_TIMEOUT", "bigquery.query_timeout"),
	Value:   30 * time.Second,
}

var bigQueryTableCreationTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "bigquery-table-creation-timeout",
	Usage:   "BigQuery table creation timeout. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("BIGQUERY_TABLE_CREATION_TIMEOUT", "bigquery.table_creation_timeout"),
	Value:   10 * time.Second,
}

var propertyIDFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "property-id",
	Usage:   "Property ID, used to satisfy interfaces required by d8a cloud. Ends up as column in the warehouse.",
	Sources: defaultSourceChain("PROPERTY_ID", "property.id"),
	Value:   "default",
}

var propertyNameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "property-name",
	Usage:   "Property name, used to satisfy interfaces required by d8a cloud. Ends up as column in the warehouse.",
	Sources: defaultSourceChain("PROPERTY_NAME", "property.name"),
	Value:   "Default property",
}

var propertySettingsSplitByUserIDFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "property-settings-split-by-user-id",
	Usage:   "When enabled, splits a session into multiple sessions when the user ID value changes between events. This ensures that events from different authenticated users are not grouped into the same session.", //nolint:lll // it's a description
	Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_USER_ID", "property.settings.split_by_user_id"),
	Value:   true,
}

var propertySettingsSplitByCampaignFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "property-settings-split-by-campaign",
	Usage:   "When enabled, splits a session into multiple sessions when the UTM campaign parameter value changes between events. This allows tracking separate sessions for different marketing campaigns within the same user visit.", //nolint:lll // it's a description
	Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_CAMPAIGN", "property.settings.split_by_campaign"),
	Value:   true,
}

var propertySettingsSplitByTimeSinceFirstEventFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:  "property-settings-split-by-time-since-first-event",
	Usage: "Splits a session into multiple sessions when the time elapsed since the first event exceeds this duration. This prevents extremely long sessions from being grouped together, creating more meaningful session boundaries.", //nolint:lll // it's a description
	Sources: defaultSourceChain(
		"PROPERTY_SETTINGS_SPLIT_BY_TIME_SINCE_FIRST_EVENT",
		"property.settings.split_by_time_since_first_event",
	),
	Value: 12 * time.Hour,
}

var propertySettingsSplitByMaxEventsFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "property-settings-split-by-max-events",
	Usage:   "Splits a session into multiple sessions when the number of events exceeds this value. This prevents sessions with excessive event counts from being stored as a single large session.", //nolint:lll // it's a description
	Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_MAX_EVENTS", "property.settings.split_by_max_events"),
	Value:   1000,
}

var monitoringEnabledFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "monitoring-enabled",
	Usage:   "Enable OpenTelemetry metrics",
	Sources: defaultSourceChain("MONITORING_ENABLED", "monitoring.enabled"),
	Value:   false,
}

var monitoringOTelEndpointFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "monitoring-otel-endpoint",
	Usage:   "OTel collector endpoint for metrics",
	Sources: defaultSourceChain("MONITORING_OTEL_ENDPOINT", "monitoring.otel_endpoint"),
	Value:   "localhost:4317",
}

var monitoringOTelExportIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "monitoring-otel-export-interval",
	Usage:   "Interval for exporting metrics to OTel collector",
	Sources: defaultSourceChain("MONITORING_OTEL_EXPORT_INTERVAL", "monitoring.otel_export_interval"),
	Value:   30 * time.Second,
}

var monitoringOTelInsecureFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "monitoring-otel-insecure",
	Usage:   "Allow insecure (non-TLS) connection to OTel collector",
	Sources: defaultSourceChain("MONITORING_OTEL_INSECURE", "monitoring.otel_insecure"),
	Value:   false,
}

var storageBoltDirectoryFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "storage-bolt-directory",
	Usage:   "Directory path where BoltDB database files are stored. This directory hosts two databases: 'bolt.db' for proto-session data, identifier metadata, and timing wheel bucket information, and 'bolt_kv.db' for key-value storage. These databases persist session state across restarts and are essential for session management functionality.", //nolint:lll // it's a description
	Sources: defaultSourceChain("STORAGE_BOLT_DIRECTORY", "storage.bolt_directory"),
	Value:   ".",
}

var storageQueueDirectoryFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "storage-queue-directory",
	Usage:   "Directory path where batched hits are stored in a filesystem-based queue before being processed by background workers. This directory acts as a persistent buffer between the receiver and the session processing pipeline.", //nolint:lll // it's a description
	Sources: defaultSourceChain("STORAGE_QUEUE_DIRECTORY", "storage.queue_directory"),
	Value:   "./queue",
}

var warehouseConfigFlags = []cli.Flag{
	warehouseDriverFlag,
	warehouseTableFlag,
	clickhouseHostFlag,
	clickhousePortFlag,
	clickhouseDatabaseFlag,
	clickhouseUsernameFlag,
	clickhousePasswordFlag,
	bigQueryProjectIDFlag,
	bigQueryDatasetNameFlag,
	bigQueryCredsJSONFlag,
	bigQueryWriterTypeFlag,
	bigQueryQueryTimeoutFlag,
	bigQueryTableCreationTimeoutFlag,
}
