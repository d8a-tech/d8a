package cmd

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"time"

	"github.com/urfave/cli-altsrc/v3/yaml"
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

var serverHostFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "server-host",
	Usage:   "Host to listen on for HTTP server",
	Sources: defaultSourceChain("SERVER_HOST", "server.host"),
	Value:   "0.0.0.0",
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

var sessionsTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "sessions-timeout",
	Usage:   "Maximum time period of inactivity after which a proto-session is considered expired and ready to be closed. The system uses a timing wheel to schedule session closures based on each hit's server received time plus this duration. After this period elapses without new hits, the proto-session is finalized and written to the warehouse as a completed session.", //nolint:lll // it's a description
	Sources: defaultSourceChain("SESSIONS_TIMEOUT", "sessions.timeout"),
	Value:   30 * time.Minute,
}

var skipCatchUpFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "skip-catch-up",
	Usage:   "When enabled, skips overdue proto-session closure catch-up on startup by rebasing the timing wheel to the current bucket instead of replaying persisted overdue buckets.", //nolint:lll // it's a description
	Sources: defaultSourceChain("SKIP_CATCH_UP", "startup.skip_catch_up"),
	Value:   false,
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
	Value:   true,
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

var currencyDestinationDirectoryFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "currency-destination-directory",
	Usage:   "Directory where downloaded currency rate snapshots are stored and reused across restarts. If no snapshot exists yet, converted currency columns will be null until a refresh succeeds.", //nolint:lll // it's a description
	Sources: defaultSourceChain("CURRENCY_DESTINATION_DIRECTORY", "currency.destination_directory"),
	Value:   "./currency",
}

var currencyRefreshIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "currency-refresh-interval",
	Usage:   "How often the application refreshes currency rate snapshots in the background. Set to 0 to disable background refreshes.", //nolint:lll // it's a description
	Sources: defaultSourceChain("CURRENCY_REFRESH_INTERVAL", "currency.refresh_interval"),
	Value:   6 * time.Hour,
}

var deviceDetectionProviderFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "device-detector-provider",
	Usage:   "Device detector provider (dd2 or stub)",
	Sources: defaultSourceChain("DEVICE_DETECTOR_PROVIDER", "device_detector.provider"),
	Value:   "dd2",
}

var warehouseDriverFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-driver",
	Usage:   "Target warehouse driver (clickhouse, bigquery, files, console, or noop)",
	Sources: defaultSourceChain("WAREHOUSE_DRIVER", "warehouse.driver"),
	Value:   "console",
}

var warehouseTableFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-table",
	Usage:   "Target warehouse table name.",
	Sources: defaultSourceChain("WAREHOUSE_TABLE", "warehouse.table"),
	Value:   "events",
}

var warehouseClickhouseHostFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-clickhouse-host",
	Usage:   "ClickHouse host. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_HOST", "warehouse.clickhouse.host"),
}

var warehouseClickhousePortFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-clickhouse-port",
	Usage:   "ClickHouse port. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PORT", "warehouse.clickhouse.port"),
	Value:   "9000",
}

var warehouseClickhouseDatabaseFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-clickhouse-database",
	Usage:   "ClickHouse database name. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_DB", "warehouse.clickhouse.database"),
}

var warehouseClickhouseUsernameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-clickhouse-username",
	Usage:   "ClickHouse username. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_USER", "warehouse.clickhouse.username"),
	Value:   "",
}

var warehouseClickhousePasswordFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-clickhouse-password",
	Usage:   "ClickHouse password. Only applicable when warehouse-driver is set to 'clickhouse'.",
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PASSWORD", "warehouse.clickhouse.password"),
	Value:   "",
}

var warehouseClickhouseOrderByFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-clickhouse-order-by",
	Usage:   "Comma-separated list of columns for ORDER BY clause (e.g., 'property_id,date_utc'). Only applicable when warehouse-driver is set to 'clickhouse'.", //nolint:lll // it's a description
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_ORDER_BY", "warehouse.clickhouse.order_by"),
	Value:   "property_id,date_utc,session_id",
}

var warehouseClickhousePartitionByFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-clickhouse-partition-by",
	Usage:   "Expression for PARTITION BY clause (e.g., 'toYYYYMM(date_utc)'). Only applicable when warehouse-driver is set to 'clickhouse'.", //nolint:lll // it's a description
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PARTITION_BY", "warehouse.clickhouse.partition_by"),
	Value:   "toYYYYMM(date_utc)",
}

// BigQuery flags
var warehouseBigQueryProjectIDFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-bigquery-project-id",
	Usage:   "BigQuery GCP project ID. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_PROJECT_ID", "warehouse.bigquery.project_id"),
}

var warehouseBigQueryDatasetNameFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-bigquery-dataset-name",
	Usage:   "BigQuery dataset name. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_DATASET_NAME", "warehouse.bigquery.dataset_name"),
}

var warehouseBigQueryCredsJSONFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-bigquery-creds-json",
	Usage:   "BigQuery service account JSON (raw or base64). Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_CREDS_JSON", "warehouse.bigquery.creds_json"),
}

var warehouseBigQueryWriterTypeFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-bigquery-writer-type",
	Usage:   "BigQuery writer type (loadjob or streaming). Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_WRITER_TYPE", "warehouse.bigquery.writer_type"),
	Value:   "loadjob",
}

var warehouseBigQueryQueryTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "warehouse-bigquery-query-timeout",
	Usage:   "BigQuery query timeout. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_QUERY_TIMEOUT", "warehouse.bigquery.query_timeout"),
	Value:   30 * time.Second,
}

var warehouseBigQueryTableCreationTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "warehouse-bigquery-table-creation-timeout",
	Usage:   "BigQuery table creation timeout. Only applicable when warehouse-driver is set to 'bigquery'.",
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_TABLE_CREATION_TIMEOUT", "warehouse.bigquery.table_creation_timeout"),
	Value:   10 * time.Second,
}

var warehouseBigQueryPartitionFieldFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-bigquery-partition-field",
	Usage:   "BigQuery partition field (top-level TIMESTAMP or DATE). By default uses date_utc column.", //nolint:lll // it's a description
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_PARTITION_FIELD", "warehouse.bigquery.partition_field"),
	Value:   "date_utc",
}

var warehouseBigQueryPartitionIntervalFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "warehouse-bigquery-partition-interval",
	Usage:   "BigQuery partition interval (HOUR, DAY, MONTH, YEAR). By default uses DAY interval.", //nolint:lll // it's a description
	Sources: defaultSourceChain("WAREHOUSE_BIGQUERY_PARTITION_INTERVAL", "warehouse.bigquery.partition_interval"),
	Value:   "DAY",
}

var warehouseBigQueryPartitionExpirationDaysFlag *cli.IntFlag = &cli.IntFlag{
	Name:  "warehouse-bigquery-partition-expiration-days",
	Usage: "BigQuery partition expiration in days. 0 means partitions do not expire. By default uses no expiration.", //nolint:lll // it's a description
	Sources: defaultSourceChain(
		"WAREHOUSE_BIGQUERY_PARTITION_EXPIRATION_DAYS",
		"warehouse.bigquery.partition_expiration_days",
	),
	Value: 0,
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

var queueBackendFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "queue-backend",
	Usage:   "Queue backend used between receiver and worker (filesystem or objectstorage)",
	Sources: defaultSourceChain("QUEUE_BACKEND", "queue.backend"),
	Value:   queueBackendFilesystem,
}

var queueObjectStorageMinIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "queue-objectstorage-min-interval",
	Usage:   "Minimum polling interval for objectstorage queue consumer (only used for objectstorage backend)",
	Sources: defaultSourceChain("QUEUE_OBJECTSTORAGE_MIN_INTERVAL", "queue.object_storage.min_interval"),
	Value:   5 * time.Second,
}

var queueObjectStorageMaxIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "queue-objectstorage-max-interval",
	Usage:   "Maximum polling interval for objectstorage queue consumer exponential backoff (only used for objectstorage backend)", //nolint:lll // it's a description
	Sources: defaultSourceChain("QUEUE_OBJECTSTORAGE_MAX_INTERVAL", "queue.object_storage.max_interval"),
	Value:   1 * time.Minute,
}

var queueObjectStorageIntervalExpFactorFlag *cli.Float64Flag = &cli.Float64Flag{
	Name:    "queue-objectstorage-interval-exp-factor",
	Usage:   "Exponential backoff factor for objectstorage queue consumer polling interval (only used for objectstorage backend)", //nolint:lll // it's a description
	Sources: defaultSourceChain("QUEUE_OBJECTSTORAGE_INTERVAL_EXP_FACTOR", "queue.object_storage.interval_exp_factor"),
	Value:   1.5,
}

var queueObjectStorageMaxItemsToReadAtOnceFlag *cli.IntFlag = &cli.IntFlag{
	Name:  "queue-objectstorage-max-items-to-read-at-once",
	Usage: "Maximum number of items to read in one batch from objectstorage queue (only used for objectstorage backend)", //nolint:lll // it's a description
	Sources: defaultSourceChain(
		"QUEUE_OBJECTSTORAGE_MAX_ITEMS_TO_READ_AT_ONCE",
		"queue.object_storage.max_items_to_read_at_once",
	),
	Value: 1000,
}

// Queue object storage flags are generated via objectStorageFlagsSpec.Queue
var queueObjectStorageCliFlags = ToCliFlags(&objectStorageFlagsSpec.Queue)

// Warehouse object storage flags are generated via objectStorageFlagsSpec.Warehouse
var warehouseObjectStorageCliFlags = ToCliFlags(&objectStorageFlagsSpec.Warehouse)

// Files warehouse flags
var (
	warehouseFilesFormatFlag = &cli.StringFlag{
		Name:    "warehouse-files-format",
		Usage:   "File format for warehouse output (csv)",
		Value:   "csv",
		Sources: defaultSourceChain("WAREHOUSE_FILES_FORMAT", "warehouse.files.format"),
	}

	warehouseFilesStorageFlag = &cli.StringFlag{
		Name:    "warehouse-files-storage",
		Usage:   "Storage destination for warehouse files (s3, gcs, or filesystem)",
		Sources: defaultSourceChain("WAREHOUSE_FILES_STORAGE", "warehouse.files.storage"),
	}

	warehouseFilesFilesystemPathFlag = &cli.StringFlag{
		Name:    "warehouse-files-filesystem-path",
		Usage:   "Destination directory for filesystem storage (required when warehouse-files-storage=filesystem)",
		Sources: defaultSourceChain("WAREHOUSE_FILES_FILESYSTEM_PATH", "warehouse.files.filesystem.path"),
	}

	warehouseFilesMaxSegmentSizeFlag = &cli.Int64Flag{
		Name:    "warehouse-files-max-segment-size",
		Usage:   "Maximum segment size in bytes before sealing (default: 1 GiB)",
		Value:   1 << 30,
		Sources: defaultSourceChain("WAREHOUSE_FILES_MAX_SEGMENT_SIZE", "warehouse.files.max_segment_size"),
	}

	warehouseFilesMaxSegmentAgeFlag = &cli.DurationFlag{
		Name:    "warehouse-files-max-segment-age",
		Usage:   "Maximum segment age before sealing (default: 1h)",
		Value:   time.Hour,
		Sources: defaultSourceChain("WAREHOUSE_FILES_MAX_SEGMENT_AGE", "warehouse.files.max_segment_age"),
	}

	warehouseFilesSealCheckIntervalFlag = &cli.DurationFlag{
		Name:    "warehouse-files-seal-check-interval",
		Usage:   "How often to evaluate sealing triggers (default: 15s)",
		Value:   15 * time.Second,
		Sources: defaultSourceChain("WAREHOUSE_FILES_SEAL_CHECK_INTERVAL", "warehouse.files.seal_check_interval"),
	}

	warehouseFilesCompressionFlag = &cli.StringFlag{
		Name:    "warehouse-files-compression",
		Usage:   "Compression algorithm for warehouse files (gzip, or empty for none)",
		Value:   "",
		Sources: defaultSourceChain("WAREHOUSE_FILES_COMPRESSION", "warehouse.files.compression"),
	}

	warehouseFilesCompressionLevelFlag = &cli.IntFlag{
		Name:    "warehouse-files-compression-level",
		Usage:   "Compression level for warehouse files (-1 = default, 1 = fastest, 9 = best compression)",
		Value:   gzip.DefaultCompression,
		Sources: defaultSourceChain("WAREHOUSE_FILES_COMPRESSION_LEVEL", "warehouse.files.compression_level"),
	}

	warehouseFilesPathTemplateFlag = &cli.StringFlag{
		Name:    "warehouse-files-path-template",
		Usage:   "Path template for warehouse file uploads. Variables: Table, Schema, SegmentID, Extension, Year, Month, MonthPadded, Day, DayPadded", //nolint:lll // it's a description
		Value:   "table={{.Table}}/schema={{.Schema}}/y={{.Year}}/m={{.MonthPadded}}/d={{.DayPadded}}/{{.SegmentID}}.{{.Extension}}",                  //nolint:lll // default template
		Sources: defaultSourceChain("WAREHOUSE_FILES_PATH_TEMPLATE", "warehouse.files.path_template"),
	}
)

var storageSpoolEnabledFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "storage-spool-enabled",
	Usage:   "Enable spooling of sessions to a filesystem-based spool before writing to the warehouse. This can improve performance by deferring the writes to the warehouse.", //nolint:lll // it's a description
	Sources: defaultSourceChain("STORAGE_SPOOL_ENABLED", "storage.spool_enabled"),
	Value:   true,
}

var storageSpoolDirectoryFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "storage-spool-directory",
	Usage:   "Directory path where sessions are stored in a filesystem-based spool before being written to the warehouse. This directory acts as a persistent buffer between the session writer and the warehouse.", //nolint:lll // it's a description
	Sources: defaultSourceChain("STORAGE_SPOOL_DIRECTORY", "storage.spool_directory"),
	Value:   "./spool",
}

var storageSpoolWriteChanBufferFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "storage-spool-write-chan-buffer",
	Usage:   "Capacity of the spool writer's input channel. Larger values reduce blocking of close path when L2 flush runs (improves close p99) at the cost of more sessions in memory on crash. Zero = unbuffered.", //nolint:lll // it's a description
	Sources: defaultSourceChain("STORAGE_SPOOL_WRITE_CHAN_BUFFER", "storage.spool_write_chan_buffer"),
	Value:   1000,
}

var protocolFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "protocol",
	Usage:   "Protocol to use for tracking requests. Valid values are 'ga4', 'd8a', 'matomo'.",
	Sources: defaultSourceChain("PROTOCOL", "protocol"),
	Value:   "ga4",
}

var matomoTrackingEndpointsFlag *cli.StringSliceFlag = &cli.StringSliceFlag{
	Name:    "matomo-tracking-endpoints",
	Usage:   "Additional Matomo tracking endpoint paths to accept besides /matomo.php. Example: /piwik.php, /tracking/matomo.php.", //nolint:lll // it's a description
	Sources: defaultSourceChain("MATOMO_TRACKING_ENDPOINTS", "matomo.tracking_endpoints"),
}

var ga4ParamsFlag *cli.StringFlag = &cli.StringFlag{
	Name: "ga4-params",
	Usage: "GA4 shortcut entries for flattening nested event params into custom columns. " +
		"Value is a JSON array string; entries from flag/env append to YAML entries. " +
		"See [Flattening nested parameters](./tracking-protocols/flattening-nested-parameters.md).",
	Sources: cli.NewValueSourceChain(
		func() cli.ValueSource {
			f := cli.EnvVars("GA4_PARAMS")
			return &f
		}(),
	),
}

var matomoCustomDimensionsFlag *cli.StringFlag = &cli.StringFlag{
	Name: "matomo-custom-dimensions",
	Usage: "Matomo custom dimension shortcut entries for flattening nested values into custom columns. " +
		"Value is a JSON array string; entries from flag/env append to YAML entries. " +
		"See [Flattening nested parameters](./tracking-protocols/flattening-nested-parameters.md).",
	Sources: cli.NewValueSourceChain(
		func() cli.ValueSource {
			f := cli.EnvVars("MATOMO_CUSTOM_DIMENSIONS")
			return &f
		}(),
	),
}

var matomoCustomVariablesFlag *cli.StringFlag = &cli.StringFlag{
	Name: "matomo-custom-variables",
	Usage: "Matomo custom variable shortcut entries for flattening nested values into custom columns. " +
		"Value is a JSON array string; entries from flag/env append to YAML entries. " +
		"See [Flattening nested parameters](./tracking-protocols/flattening-nested-parameters.md).",
	Sources: cli.NewValueSourceChain(
		func() cli.ValueSource {
			f := cli.EnvVars("MATOMO_CUSTOM_VARIABLES")
			return &f
		}(),
	),
}

var telemetryURLFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "telemetry-url",
	Usage:   "Telemetry endpoint URL for sending usage events. Anonymous and non-invasive: collects only app version and runtime duration. Client ID (UUID) is generated per app start and not persisted, resetting on each restart. If empty, telemetry is disabled.", //nolint:lll // it's a description
	Sources: defaultSourceChain("TELEMETRY_URL", "telemetry.url"),
	Value:   "https://global.t.d8a.tech/28b4fbc6-a4d0-49c4-883f-58314f83416e/g/collect",
}

var filtersFieldsFlag *cli.StringSliceFlag = &cli.StringSliceFlag{
	Name: "filters-fields",
	Usage: "Array of field names to make available to filter expressions. Can contain any event-scoped column names. " + //nolint:lll // it's a description
		"These fields are injected into the expression environment and can be referenced in filter condition expressions. " +
		"Example: ip_address, event_name, user_id, page_location. The default value includes ip_address for backward compatibility. " + //nolint:lll // it's a description
		"See [Traffic filtering](./traffic-filtering.md) for details.",
	Sources: defaultSourceChain("FILTERS_FIELDS", "filters.fields"),
	Value:   []string{"ip_address"},
}

// unusedConfigSourcer implements altsrc.Sourcer to point to a non-existent config file.
// This prevents parsing actual YAML values while still showing the config path in docs.
type unusedConfigSourcer struct{}

func (u *unusedConfigSourcer) SourceURI() string {
	return "./tmp/d8a_filters_conditions_unused.yaml"
}

var filtersConditionsFlag *cli.StringSliceFlag = &cli.StringSliceFlag{
	Name: "filters-conditions",
	Usage: "Array of filter conditions for traffic filtering. Each condition is a JSON-encoded string with fields: " + //nolint:lll // it's a description
		"'name' (string identifier), 'type' (exclude or allow), 'test_mode' (boolean), 'expression' (filter expression). " +
		"Example: `{\"name\":\"internal_traffic\",\"type\":\"exclude\",\"test_mode\":false,\"expression\":\"ip_address == '10.0.0.1'\"}`. " + //nolint:lll // it's a description
		"Can be set via CLI flag, environment variable (FILTERS_CONDITIONS), or YAML config (filters.conditions). " +
		"Conditions from flag/env are appended to YAML conditions. " +
		"See [Traffic filtering](./traffic-filtering.md) for details.",
	Sources: cli.NewValueSourceChain(
		func() cli.ValueSource {
			f := cli.EnvVars("FILTERS_CONDITIONS")
			return &f
		}(),
		yaml.YAML("filters.conditions", &unusedConfigSourcer{}),
	),
}

var warehouseConfigFlags = []cli.Flag{
	warehouseDriverFlag,
	warehouseTableFlag,
	warehouseClickhouseHostFlag,
	warehouseClickhousePortFlag,
	warehouseClickhouseDatabaseFlag,
	warehouseClickhouseUsernameFlag,
	warehouseClickhousePasswordFlag,
	warehouseClickhouseOrderByFlag,
	warehouseClickhousePartitionByFlag,
	warehouseBigQueryProjectIDFlag,
	warehouseBigQueryDatasetNameFlag,
	warehouseBigQueryCredsJSONFlag,
	warehouseBigQueryWriterTypeFlag,
	warehouseBigQueryQueryTimeoutFlag,
	warehouseBigQueryTableCreationTimeoutFlag,
	warehouseBigQueryPartitionFieldFlag,
	warehouseBigQueryPartitionIntervalFlag,
	warehouseBigQueryPartitionExpirationDaysFlag,
	warehouseFilesFormatFlag,
	warehouseFilesStorageFlag,
	warehouseFilesFilesystemPathFlag,
	warehouseFilesMaxSegmentSizeFlag,
	warehouseFilesMaxSegmentAgeFlag,
	warehouseFilesSealCheckIntervalFlag,
	warehouseFilesCompressionFlag,
	warehouseFilesCompressionLevelFlag,
	warehouseFilesPathTemplateFlag,
}

func getServerFlags() []cli.Flag {
	return mergeFlags(
		[]cli.Flag{
			serverHostFlag,
			serverPortFlag,
			receiverBatchSizeFlag,
			receiverBatchTimeoutFlag,
			receiverMaxHitKbytesFlag,
			sessionsTimeoutFlag,
			skipCatchUpFlag,
			sessionsJoinBySessionStampFlag,
			sessionsJoinByUserIDFlag,
			dbipEnabled,
			dbipDestinationDirectory,
			dbipDownloadTimeoutFlag,
			currencyDestinationDirectoryFlag,
			currencyRefreshIntervalFlag,
			deviceDetectionProviderFlag,
			propertyIDFlag,
			propertyNameFlag,
			propertySettingsSplitByUserIDFlag,
			propertySettingsSplitByCampaignFlag,
			protocolFlag,
			matomoTrackingEndpointsFlag,
			ga4ParamsFlag,
			matomoCustomDimensionsFlag,
			matomoCustomVariablesFlag,
			propertySettingsSplitByTimeSinceFirstEventFlag,
			propertySettingsSplitByMaxEventsFlag,
			monitoringEnabledFlag,
			monitoringOTelEndpointFlag,
			monitoringOTelExportIntervalFlag,
			monitoringOTelInsecureFlag,
			storageBoltDirectoryFlag,
			storageQueueDirectoryFlag,
			queueBackendFlag,
			queueObjectStorageMinIntervalFlag,
			queueObjectStorageMaxIntervalFlag,
			queueObjectStorageIntervalExpFactorFlag,
			queueObjectStorageMaxItemsToReadAtOnceFlag,
			storageSpoolEnabledFlag,
			storageSpoolDirectoryFlag,
			storageSpoolWriteChanBufferFlag,
			telemetryURLFlag,
			filtersFieldsFlag,
			filtersConditionsFlag,
		},
		queueObjectStorageCliFlags,
		warehouseObjectStorageCliFlags,
		warehouseConfigFlags,
	)
}
