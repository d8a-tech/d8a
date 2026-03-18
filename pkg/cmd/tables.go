package cmd

import (
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/customcolumns"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

type tables struct {
	events               string
	sessionsColumnPrefix string
}

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
	psr := propertySettings(cmd)
	settings, err := psr.GetByPropertyID(cmd.String(propertyIDFlag.Name))
	if err != nil {
		logrus.Panicf("failed to get property settings: %v", err)
	}
	protocol := protocolByID(settings.ProtocolID, cmd)
	if protocol == nil {
		logrus.Panicf("protocol %s not found", settings.ProtocolID)
	}
	crLock.Lock()
	defer crLock.Unlock()
	if cr == nil {
		var opts []columnset.ColumnSetOption

		if cmd.Bool(dbipEnabled.Name) {
			geoColumns := dbip.GeoColumns(
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
			opts = append(opts, columnset.WithGeoIPColumns(geoColumns))
		}

		deviceProvider := cmd.String(deviceDetectionProviderFlag.Name)
		switch deviceProvider {
		case "dd2":
			opts = append(opts, columnset.WithDeviceDetectionColumns(eventcolumns.DD2Columns()))
		case "stub":
			// Do nothing - use default stubs
		default:
			logrus.Panicf("invalid device-detection-provider value: %s (must be 'dd2' or 'stub')", deviceProvider)
		}

		opts = append(opts, columnset.WithCustomColumnsRegistry(
			customcolumns.NewCustomColumnsPropertySettingsRegistry(psr, customcolumns.NewBuilder()),
		))

		cr = columnset.DefaultColumnRegistry(
			protocol,
			psr,
			opts...,
		)
	}
	return cr
}
