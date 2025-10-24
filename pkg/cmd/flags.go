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
