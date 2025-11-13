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
	Value:   5 * time.Second,
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
	Usage:   "Target warehouse driver (console or clickhouse)",
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

var warehouseConfigFlags = []cli.Flag{
	warehouseFlag,
	clickhouseHostFlag,
	clickhousePortFlag,
	clickhouseDatabaseFlag,
	clickhouseUsernameFlag,
	clickhousePasswordFlag,
}
