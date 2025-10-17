package cmd

import (
	"fmt"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/urfave/cli/v2"
)

type tables struct {
	events               string
	sessionsColumnPrefix string
}

// generateDescendingSortableID generates an identifier that sorts in descending order
// by subtracting the current timestamp from a fixed timestamp.
// This ensures newer identifiers appear first when sorted ascending.
func generateDescendingSortableID() string {
	// Fixed timestamp far in the future (Unix timestamp for ~2035)
	// This should be larger than any reasonable current timestamp
	fixedTimestamp := int64(2071711709)

	currentTimestamp := time.Now().Unix()
	descendingID := fixedTimestamp - currentTimestamp

	// Format with leading zeros to ensure constant length (10 digits)
	return fmt.Sprintf("%010d", descendingID)
}

var id = generateDescendingSortableID()

// getTableNames returns the table names for the current property.
func getTableNames() tables {
	return tables{
		events:               fmt.Sprintf("events_%s", id),
		sessionsColumnPrefix: "",
	}
}

var crLock = sync.Mutex{}
var cr schema.ColumnsRegistry

func columnsRegistry(c *cli.Context) schema.ColumnsRegistry {
	crLock.Lock()
	defer crLock.Unlock()
	if cr == nil {
		var geoColumns []schema.EventColumn
		if c.Bool(dbipEnabled.Name) {
			geoColumns = dbip.GeoColumns(
				dbip.NewExtensionBasedOCIDownloader(
					dbip.OCIRegistryCreds{
						Repo:       "ghcr.io/d8a-tech",
						IgnoreCert: false,
					},
					".mmdb",
				),
				c.String(dbipDestinationDirectory.Name),
			)
		}
		cr = columnset.DefaultColumnRegistry(
			ga4.NewGA4Protocol(currencyConverter),
			geoColumns,
		)
	}
	return cr
}
