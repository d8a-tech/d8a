package cmd

import (
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

type tables struct {
	events               string
	sessionsColumnPrefix string
}

// getTableNames returns the table names for the current property.
func getTableNames(cmd *cli.Command) tables {
	table := cmd.String(warehouseTableFlag.Name)
	if table == "" {
		logrus.Fatalf("warehouse-table must be non-empty")
	}

	return tables{
		events:               table,
		sessionsColumnPrefix: "",
	}
}

var crLock = sync.Mutex{}
var cr schema.ColumnsRegistry

func columnsRegistry(cmd *cli.Command) schema.ColumnsRegistry {
	crLock.Lock()
	defer crLock.Unlock()
	if cr == nil {
		var geoColumns []schema.EventColumn
		if cmd.Bool(dbipEnabled.Name) {
			geoColumns = dbip.GeoColumns(
				dbip.NewExtensionBasedOCIDownloader(
					dbip.OCIRegistryCreds{
						Repo:       "ghcr.io/d8a-tech",
						IgnoreCert: false,
					},
					".mmdb",
				),
				cmd.String(dbipDestinationDirectory.Name),
				cmd.Duration(dbipDownloadTimeoutFlag.Name),
				dbip.CacheConfig{
					MaxEntries: 1024,
					TTL:        30 * time.Second,
				},
			)
		}
		cr = columnset.DefaultColumnRegistry(
			ga4.NewGA4Protocol(currencyConverter, propertySource(cmd)),
			geoColumns,
			propertySource(cmd),
		)
	}
	return cr
}
