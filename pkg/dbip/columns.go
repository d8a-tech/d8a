package dbip

import (
	"context"
	"net/netip"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

func CityColumn(d MMDBCityDatabaseDownloader) schema.EventColumn {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mmdb, err := d.Download(ctx, "dbip-city-lite", "latest", "/tmp")
	if err != nil {
		logrus.WithError(err).Panic("failed to download MMDB city database")
	}
	db, err := maxminddb.Open(mmdb)
	if err != nil {
		logrus.WithError(err).Panic("failed to open MMDB city database")
	}
	defer db.Close()

	return columns.NewSimpleEventColumn(
		columns.CoreInterfaces.GeoCity.ID,
		columns.CoreInterfaces.GeoCity.Field,
		func(event *schema.Event) (any, error) {
			ipAddress := netip.MustParseAddr(event.BoundHit.IP)
			var record any
			err = db.Lookup(ipAddress).Decode(&record)
			if err != nil {
				return nil, err
			}
			return record, nil
		},
	)
}
