package sessioncolumns

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

func dominantClientIDFirstEventValueColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	source schema.Interface,
	displayName string,
	description string,
) schema.SessionColumn {
	return columns.FirstEventValueFromMostFrequentClientIDColumn(
		id,
		field,
		columns.ExctractFieldValue(source.Field.Name),
		columns.WithSessionColumnDependsOn(
			schema.DependsOnEntry{Interface: source.ID},
		),
		columns.WithSessionColumnDocs(displayName, description),
	)
}

var SessionGeoCityColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionGeoCity.ID,
	columns.CoreInterfaces.SessionGeoCity.Field,
	columns.CoreInterfaces.GeoCity,
	"Session Geo City",
	"The city from the first event of the client ID contributing the most events in the session.",
)

var SessionGeoRegionColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionGeoRegion.ID,
	columns.CoreInterfaces.SessionGeoRegion.Field,
	columns.CoreInterfaces.GeoRegion,
	"Session Geo Region",
	"The region from the first event of the client ID contributing the most events in the session.",
)

var SessionGeoMetroColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionGeoMetro.ID,
	columns.CoreInterfaces.SessionGeoMetro.Field,
	columns.CoreInterfaces.GeoMetro,
	"Session Geo Metro",
	"The metro area from the first event of the client ID contributing the most events in the session.",
)

var SessionGeoCountryColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionGeoCountry.ID,
	columns.CoreInterfaces.SessionGeoCountry.Field,
	columns.CoreInterfaces.GeoCountry,
	"Session Geo Country",
	"The country from the first event of the client ID contributing the most events in the session.",
)

var SessionGeoContinentColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionGeoContinent.ID,
	columns.CoreInterfaces.SessionGeoContinent.Field,
	columns.CoreInterfaces.GeoContinent,
	"Session Geo Continent",
	"The continent from the first event of the client ID contributing the most events in the session.",
)

var SessionGeoSubContinentColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionGeoSubContinent.ID,
	columns.CoreInterfaces.SessionGeoSubContinent.Field,
	columns.CoreInterfaces.GeoSubContinent,
	"Session Geo Sub-Continent",
	"The sub-continent from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceCategoryColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceCategory.ID,
	columns.CoreInterfaces.SessionDeviceCategory.Field,
	columns.CoreInterfaces.DeviceCategory,
	"Session Device Category",
	"The device category from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceLanguageColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceLanguage.ID,
	columns.CoreInterfaces.SessionDeviceLanguage.Field,
	columns.CoreInterfaces.DeviceLanguage,
	"Session Device Language",
	"The device language from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceMobileBrandNameColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceMobileBrandName.ID,
	columns.CoreInterfaces.SessionDeviceMobileBrandName.Field,
	columns.CoreInterfaces.DeviceMobileBrandName,
	"Session Device Brand (mobile)",
	"The mobile device brand name from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceMobileModelNameColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceMobileModelName.ID,
	columns.CoreInterfaces.SessionDeviceMobileModelName.Field,
	columns.CoreInterfaces.DeviceMobileModelName,
	"Session Device Model (mobile)",
	"The mobile device model name from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceOperatingSystemColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceOperatingSystem.ID,
	columns.CoreInterfaces.SessionDeviceOperatingSystem.Field,
	columns.CoreInterfaces.DeviceOperatingSystem,
	"Session Operating System",
	"The operating system from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceOperatingSystemVersionColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceOperatingSystemVersion.ID,
	columns.CoreInterfaces.SessionDeviceOperatingSystemVersion.Field,
	columns.CoreInterfaces.DeviceOperatingSystemVersion,
	"Session Operating System Version",
	"The operating system version from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceWebBrowserColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceWebBrowser.ID,
	columns.CoreInterfaces.SessionDeviceWebBrowser.Field,
	columns.CoreInterfaces.DeviceWebBrowser,
	"Session Web Browser",
	"The web browser from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceWebBrowserVersionColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceWebBrowserVersion.ID,
	columns.CoreInterfaces.SessionDeviceWebBrowserVersion.Field,
	columns.CoreInterfaces.DeviceWebBrowserVersion,
	"Session Web Browser Version",
	"The web browser version from the first event of the client ID contributing the most events in the session.",
)

var SessionDeviceScreenResolutionColumn = dominantClientIDFirstEventValueColumn(
	columns.CoreInterfaces.SessionDeviceScreenResolution.ID,
	columns.CoreInterfaces.SessionDeviceScreenResolution.Field,
	columns.CoreInterfaces.DeviceScreenResolution,
	"Session Device Screen Resolution",
	"The screen resolution from the first event of the client ID contributing the most events in the session.",
)
