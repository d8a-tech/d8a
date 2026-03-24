package cmd

import (
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

func buildDBIPProvider(cmd *cli.Command) (dbip.ManagedLookupProvider, func(), error) {
	provider := dbip.NewManagedLookupProvider(
		dbip.WithEnabled(cmd.Bool(dbipEnabled.Name)),
		dbip.WithDestination(cmd.String(dbipDestinationDirectory.Name)),
		dbip.WithDownloadTimeout(cmd.Duration(dbipDownloadTimeoutFlag.Name)),
		dbip.WithRefreshInterval(cmd.Duration(dbipRefreshIntervalFlag.Name)),
		dbip.WithDownloader(
			dbip.NewExtensionBasedOCIDownloader(
				dbip.OCIRegistryCreds{
					Repo:       "ghcr.io/d8a-tech",
					IgnoreCert: false,
				},
				".mmdb",
			),
		),
	)

	cleanup := func() {
		if closeErr := provider.Close(); closeErr != nil {
			logrus.WithError(closeErr).Error("close dbip provider")
		}
	}

	return provider, cleanup, nil
}
