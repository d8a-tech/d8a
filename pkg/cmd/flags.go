package cmd

import (
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

var clickhouseOrderByFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-order-by",
	Usage:   "Comma-separated list of columns for ORDER BY clause (e.g., 'property_id,date_utc'). Only applicable when warehouse-driver is set to 'clickhouse'.", //nolint:lll // it's a description
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_ORDER_BY", "clickhouse.order_by"),
	Value:   "property_id,date_utc,session_id",
}

var clickhousePartitionByFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "clickhouse-partition-by",
	Usage:   "Expression for PARTITION BY clause (e.g., 'toYYYYMM(date_utc)'). Only applicable when warehouse-driver is set to 'clickhouse'.", //nolint:lll // it's a description
	Sources: defaultSourceChain("WAREHOUSE_CLICKHOUSE_PARTITION_BY", "clickhouse.partition_by"),
	Value:   "toYYYYMM(date_utc)",
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

var bigQueryPartitionFieldFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-partition-field",
	Usage:   "BigQuery partition field (top-level TIMESTAMP or DATE). By default uses date_utc column.", //nolint:lll // it's a description
	Sources: defaultSourceChain("BIGQUERY_PARTITION_FIELD", "bigquery.partition_field"),
	Value:   "date_utc",
}

var bigQueryPartitionIntervalFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "bigquery-partition-interval",
	Usage:   "BigQuery partition interval (HOUR, DAY, MONTH, YEAR). By default uses DAY interval.", //nolint:lll // it's a description
	Sources: defaultSourceChain("BIGQUERY_PARTITION_INTERVAL", "bigquery.partition_interval"),
	Value:   "DAY",
}

var bigQueryPartitionExpirationDaysFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "bigquery-partition-expiration-days",
	Usage:   "BigQuery partition expiration in days. 0 means partitions do not expire. By default uses no expiration.", //nolint:lll // it's a description
	Sources: defaultSourceChain("BIGQUERY_PARTITION_EXPIRATION_DAYS", "bigquery.partition_expiration_days"),
	Value:   0,
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

var queueObjectPrefixFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "queue-object-prefix",
	Usage:   "Object storage prefix/namespace for queue objects (only used for objectstorage backend)",
	Sources: defaultSourceChain("QUEUE_OBJECT_PREFIX", "queue.object_prefix"),
	Value:   "d8a/queue",
}

var queueObjectStorageMinIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "queue-objectstorage-min-interval",
	Usage:   "Minimum polling interval for objectstorage queue consumer (only used for objectstorage backend)",
	Sources: defaultSourceChain("QUEUE_OBJECTSTORAGE_MIN_INTERVAL", "queue.objectstorage_min_interval"),
	Value:   5 * time.Second,
}

var queueObjectStorageMaxIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "queue-objectstorage-max-interval",
	Usage:   "Maximum polling interval for objectstorage queue consumer exponential backoff (only used for objectstorage backend)", //nolint:lll // it's a description
	Sources: defaultSourceChain("QUEUE_OBJECTSTORAGE_MAX_INTERVAL", "queue.objectstorage_max_interval"),
	Value:   1 * time.Minute,
}

var queueObjectStorageIntervalExpFactorFlag *cli.Float64Flag = &cli.Float64Flag{
	Name:    "queue-objectstorage-interval-exp-factor",
	Usage:   "Exponential backoff factor for objectstorage queue consumer polling interval (only used for objectstorage backend)", //nolint:lll // it's a description
	Sources: defaultSourceChain("QUEUE_OBJECTSTORAGE_INTERVAL_EXP_FACTOR", "queue.objectstorage_interval_exp_factor"),
	Value:   1.5,
}

var queueObjectStorageMaxItemsToReadAtOnceFlag *cli.IntFlag = &cli.IntFlag{
	Name:  "queue-objectstorage-max-items-to-read-at-once",
	Usage: "Maximum number of items to read in one batch from objectstorage queue (only used for objectstorage backend)", //nolint:lll // it's a description
	Sources: defaultSourceChain(
		"QUEUE_OBJECTSTORAGE_MAX_ITEMS_TO_READ_AT_ONCE",
		"queue.objectstorage_max_items_to_read_at_once",
	),
	Value: 1000,
}

var objectStorageTypeFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-type",
	Usage:   "Object storage type (s3 or gcs)",
	Sources: defaultSourceChain("OBJECT_STORAGE_TYPE", "object_storage.type"),
}

var objectStorageS3HostFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-s3-host",
	Usage:   "S3/MinIO host (only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_HOST", "object_storage.s3.host"),
}

var objectStorageS3PortFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "object-storage-s3-port",
	Usage:   "S3/MinIO port (only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_PORT", "object_storage.s3.port"),
	Value:   9000,
}

var objectStorageS3BucketFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-s3-bucket",
	Usage:   "S3/MinIO bucket name (only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_BUCKET", "object_storage.s3.bucket"),
}

var objectStorageS3AccessKeyFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-s3-access-key",
	Usage:   "S3/MinIO access key (only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_ACCESS_KEY", "object_storage.s3.access_key"),
}

var objectStorageS3SecretKeyFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-s3-secret-key",
	Usage:   "S3/MinIO secret key (only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_SECRET_KEY", "object_storage.s3.secret_key"),
}

var objectStorageS3RegionFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-s3-region",
	Usage:   "S3 region (only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_REGION", "object_storage.s3.region"),
	Value:   "us-east-1",
}

var objectStorageS3ProtocolFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-s3-protocol",
	Usage:   "S3 endpoint protocol (http or https; only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_PROTOCOL", "object_storage.s3.protocol"),
	Value:   "http",
}

var objectStorageS3CreateBucketFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "object-storage-s3-create-bucket",
	Usage:   "Create bucket on startup if missing (only used when object-storage-type=s3)",
	Sources: defaultSourceChain("OBJECT_STORAGE_S3_CREATE_BUCKET", "object_storage.s3.create_bucket"),
	Value:   false,
}

var objectStorageGCSBucketFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-gcs-bucket",
	Usage:   "GCS bucket name (only used when object-storage-type=gcs)",
	Sources: defaultSourceChain("OBJECT_STORAGE_GCS_BUCKET", "object_storage.gcs.bucket"),
}

var objectStorageGCSProjectFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-gcs-project",
	Usage:   "GCS project ID (optional; only used when object-storage-type=gcs)",
	Sources: defaultSourceChain("OBJECT_STORAGE_GCS_PROJECT", "object_storage.gcs.project"),
}

var objectStorageGCSCredsJSONFlag *cli.StringFlag = &cli.StringFlag{
	Name:    "object-storage-gcs-creds-json",
	Usage:   "GCS credentials JSON (raw or base64); empty uses ADC (only used when object-storage-type=gcs)",
	Sources: defaultSourceChain("OBJECT_STORAGE_GCS_CREDS_JSON", "object_storage.gcs.creds_json"),
}

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
	Usage:   "Protocol to use for tracking requests. Valid values are 'ga4', 'd8a'.",
	Sources: defaultSourceChain("PROTOCOL", "protocol"),
	Value:   "ga4",
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
	return "/tmp/d8a_filters_conditions_unused.yaml"
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
	clickhouseHostFlag,
	clickhousePortFlag,
	clickhouseDatabaseFlag,
	clickhouseUsernameFlag,
	clickhousePasswordFlag,
	clickhouseOrderByFlag,
	clickhousePartitionByFlag,
	bigQueryProjectIDFlag,
	bigQueryDatasetNameFlag,
	bigQueryCredsJSONFlag,
	bigQueryWriterTypeFlag,
	bigQueryQueryTimeoutFlag,
	bigQueryTableCreationTimeoutFlag,
	bigQueryPartitionFieldFlag,
	bigQueryPartitionIntervalFlag,
	bigQueryPartitionExpirationDaysFlag,
}

func getServerFlags() []cli.Flag {
	return mergeFlags(
		[]cli.Flag{
			serverPortFlag,
			receiverBatchSizeFlag,
			receiverBatchTimeoutFlag,
			receiverMaxHitKbytesFlag,
			sessionsTimeoutFlag,
			sessionsJoinBySessionStampFlag,
			sessionsJoinByUserIDFlag,
			dbipEnabled,
			dbipDestinationDirectory,
			dbipDownloadTimeoutFlag,
			propertyIDFlag,
			propertyNameFlag,
			propertySettingsSplitByUserIDFlag,
			propertySettingsSplitByCampaignFlag,
			protocolFlag,
			propertySettingsSplitByTimeSinceFirstEventFlag,
			propertySettingsSplitByMaxEventsFlag,
			monitoringEnabledFlag,
			monitoringOTelEndpointFlag,
			monitoringOTelExportIntervalFlag,
			monitoringOTelInsecureFlag,
			storageBoltDirectoryFlag,
			storageQueueDirectoryFlag,
			queueBackendFlag,
			queueObjectPrefixFlag,
			queueObjectStorageMinIntervalFlag,
			queueObjectStorageMaxIntervalFlag,
			queueObjectStorageIntervalExpFactorFlag,
			queueObjectStorageMaxItemsToReadAtOnceFlag,
			objectStorageTypeFlag,
			objectStorageS3HostFlag,
			objectStorageS3PortFlag,
			objectStorageS3BucketFlag,
			objectStorageS3AccessKeyFlag,
			objectStorageS3SecretKeyFlag,
			objectStorageS3RegionFlag,
			objectStorageS3ProtocolFlag,
			objectStorageS3CreateBucketFlag,
			objectStorageGCSBucketFlag,
			objectStorageGCSProjectFlag,
			objectStorageGCSCredsJSONFlag,
			storageSpoolEnabledFlag,
			storageSpoolDirectoryFlag,
			storageSpoolWriteChanBufferFlag,
			telemetryURLFlag,
			filtersFieldsFlag,
			filtersConditionsFlag,
		},
		warehouseConfigFlags,
	)
}
