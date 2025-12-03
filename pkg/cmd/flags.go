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

var batcherBatchSizeFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "batcher-batch-size",
	Usage:   "Batch size for the batcher",
	Sources: defaultSourceChain("BATCHER_BATCH_SIZE", "batcher.batch_size"),
	Value:   5000,
}

var batcherBatchTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "batcher-batch-timeout",
	Usage:   "Batch timeout for the batcher",
	Sources: defaultSourceChain("BATCHER_BATCH_TIMEOUT", "batcher.batch_timeout"),
	Value:   1 * time.Second,
}

var closerSessionDurationFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "closer-session-duration",
	Usage:   "Session duration for the closer",
	Sources: defaultSourceChain("CLOSER_SESSION_DURATION", "closer.session_duration"),
	Value:   1 * time.Minute,
}

var closerSkipCatchingUpFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "closer-skip-catching-up",
	Usage:   "If set, the closer will skip the catching up process",
	Sources: defaultSourceChain("CLOSER_SKIP_CATCHING_UP", "closer.skip_catching_up"),
	Value:   false,
}

var closerTickIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "closer-tick-interval",
	Usage:   "Tick interval for the closer",
	Sources: defaultSourceChain("CLOSER_TICK_INTERVAL", "closer.tick_interval"),
	Value:   1 * time.Second,
}

var closerSessionJoinBySessionStampFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "closer-session-join-by-session-stamp",
	Usage:   "If set, sessions will be joined by session stamp",
	Sources: defaultSourceChain("CLOSER_SESSION_JOIN_BY_SESSION_STAMP", "closer.session_join_by_session_stamp"),
	Value:   true,
}

var closerSessionJoinByUserIDFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "closer-session-join-by-user-id",
	Usage:   "If set, sessions will be joined by user ID",
	Sources: defaultSourceChain("CLOSER_SESSION_JOIN_BY_USER_ID", "closer.session_join_by_user_id"),
	Value:   false,
}

var dbipEnabled *cli.BoolFlag = &cli.BoolFlag{
	Name:    "dbip-enabled",
	Usage:   "Use DBIP columns",
	Sources: defaultSourceChain("DBIP_ENABLED", "dbip.enabled"),
	Value:   false,
}

var dbipDestinationDirectory *cli.StringFlag = &cli.StringFlag{
	Name:    "dbip-destination-directory",
	Usage:   "Destination directory for the DBIP files used by the DBIP columns",
	Sources: defaultSourceChain("DBIP_DESTINATION_DIRECTORY", "dbip.destination_directory"),
	Value:   filepath.Join(os.TempDir(), "dbip"),
}

var dbipDownloadTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "dbip-download-timeout",
	Usage:   "Timeout for the DBIP download",
	Sources: defaultSourceChain("DBIP_DOWNLOAD_TIMEOUT", "dbip.download_timeout"),
	Value:   60 * time.Second,
}

var warehouseFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse",
	Usage:   "Target warehouse driver (console, clickhouse, or bigquery)",
	Sources: defaultSourceChain("WAREHOUSE", "warehouse"),
	Value:   "console",
}

var clickhouseHostFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-host",
	Usage:   "ClickHouse host",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_HOST", "clickhouse.host"),
}

var clickhousePortFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-port",
	Usage:   "ClickHouse port",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PORT", "clickhouse.port"),
	Value:   "9000",
}

var clickhouseDatabaseFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-database",
	Usage:   "ClickHouse database name",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_DB", "clickhouse.database"),
}

var clickhouseUsernameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-username",
	Usage:   "ClickHouse username",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_USER", "clickhouse.username"),
	Value:   "",
}

var clickhousePasswordFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-password",
	Usage:   "ClickHouse password",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PASSWORD", "clickhouse.password"),
	Value:   "",
}

// BigQuery flags
var bigQueryProjectIDFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-project-id",
	Usage:   "BigQuery GCP project ID",
	Sources: defaultSourceChain("BIGQUERY_PROJECT_ID", "bigquery.project_id"),
}

var bigQueryDatasetNameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-dataset-name",
	Usage:   "BigQuery dataset name",
	Sources: defaultSourceChain("BIGQUERY_DATASET_NAME", "bigquery.dataset_name"),
}

var bigQueryCredsJSONFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-creds-json",
	Usage:   "BigQuery service account JSON (raw or base64)",
	Sources: defaultSourceChain("BIGQUERY_CREDS_JSON", "bigquery.creds_json"),
}

var bigQueryWriterTypeFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-writer-type",
	Usage:   "BigQuery writer type (loadjob or streaming)",
	Sources: defaultSourceChain("BIGQUERY_WRITER_TYPE", "bigquery.writer_type"),
	Value:   "loadjob",
}

var bigQueryQueryTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "bigquery-query-timeout",
	Usage:   "BigQuery query timeout",
	Sources: defaultSourceChain("BIGQUERY_QUERY_TIMEOUT", "bigquery.query_timeout"),
	Value:   30 * time.Second,
}

var bigQueryTableCreationTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "bigquery-table-creation-timeout",
	Usage:   "BigQuery table creation timeout",
	Sources: defaultSourceChain("BIGQUERY_TABLE_CREATION_TIMEOUT", "bigquery.table_creation_timeout"),
	Value:   10 * time.Second,
}

var propertyIDFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "property-id",
	Usage:   "Property ID, used as a source for specific columns",
	Sources: defaultSourceChain("PROPERTY_ID", "property.id"),
	Value:   "-",
}

var propertyNameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "property-name",
	Usage:   "Property name, used as a source for specific columns",
	Sources: defaultSourceChain("PROPERTY_NAME", "property.name"),
	Value:   "Unknown Property",
}

var propertySettingsSplitByUserIDFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "property-settings-split-by-user-id",
	Usage:   "If set, the sessions will be split when the user ID value changes",
	Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_USER_ID", "property.settings.split_by_user_id"),
	Value:   true,
}

var propertySettingsSplitByCampaignFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "property-settings-split-by-campaign",
	Usage:   "If set, the sessions will be split when the UTM campaign value changes",
	Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_CAMPAIGN", "property.settings.split_by_campaign"),
	Value:   true,
}

var propertySettingsSplitByTimeSinceFirstEventFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:  "property-settings-split-by-time-since-first-event",
	Usage: "The sessions will be split when the time since first event is greater than the duration",
	Sources: defaultSourceChain(
		"PROPERTY_SETTINGS_SPLIT_BY_TIME_SINCE_FIRST_EVENT",
		"property.settings.split_by_time_since_first_event",
	),
	Value: 12 * time.Hour,
}

var propertySettingsSplitByMaxEventsFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "property-settings-split-by-max-events",
	Usage:   "The sessions will be split when the number of events is greater than the value",
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

var storageBoltDatabasePathFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "storage-bolt-database-path",
	Usage:   "Path to the Bolt database file",
	Sources: defaultSourceChain("STORAGE_BOLT_DATABASE_PATH", "storage.bolt_database_path"),
	Value:   "./bolt.db",
}

var storageQueueDirectoryFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "storage-queue-directory",
	Usage:   "Directory for the queue storage",
	Sources: defaultSourceChain("STORAGE_QUEUE_DIRECTORY", "storage.queue_directory"),
	Value:   "./queue",
}

var warehouseConfigFlags = []cli.Flag{
	warehouseFlag,
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
