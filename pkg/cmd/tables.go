package cmd

import (
	"fmt"
	"sync"

	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/currency"
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
var cr map[string]schema.ColumnsRegistry

func columnsRegistry(
	cmd *cli.Command,
	converter currency.Converter,
	geoProvider dbip.LookupProvider,
) schema.ColumnsRegistry {
	psr := propertySettings(cmd)
	settings, err := psr.GetByPropertyID(cmd.String(propertyIDFlag.Name))
	if err != nil {
		logrus.Panicf("failed to get property settings: %v", err)
	}
	protocol := protocolByID(settings.ProtocolID, cmd, converter)
	if protocol == nil {
		logrus.Panicf("protocol %s not found", settings.ProtocolID)
	}
	crLock.Lock()
	defer crLock.Unlock()
	if cr == nil {
		cr = make(map[string]schema.ColumnsRegistry)
	}

	cacheKey := settings.ProtocolID + ":" + fmt.Sprintf("%T", converter)
	cacheKey += ":" + fmt.Sprintf("%p", geoProvider)
	if registry, ok := cr[cacheKey]; ok {
		return registry
	}

	var opts []columnset.ColumnSetOption
	opts = append(opts, columnset.WithGeoProvider(geoProvider))

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

	registry := columnset.DefaultColumnRegistry(
		protocol,
		psr,
		opts...,
	)
	cr[cacheKey] = registry
	return registry
}
