package eventcolumns

import "github.com/d8a-tech/d8a/pkg/columns"

var deviceStubDescription = "Stub column, always returns null, since your device detection provider does not implement this column." // nolint:lll // it's a description

// DeviceCategoryStubColumn is the column for the device category of an event
var DeviceCategoryStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.DeviceCategory.ID,
	columns.CoreInterfaces.DeviceCategory.Field,
	columns.WithEventColumnDocs(
		"Device Category",
		deviceStubDescription,
	),
)

// DeviceMobileBrandNameStubColumn is the column for the device mobile brand name of an event
var DeviceMobileBrandNameStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.DeviceMobileBrandName.ID,
	columns.CoreInterfaces.DeviceMobileBrandName.Field,
	columns.WithEventColumnDocs(
		"Device Brand (mobile)",
		deviceStubDescription,
	),
)

// DeviceMobileModelNameStubColumn is the column for the device mobile model name of an event
var DeviceMobileModelNameStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.DeviceMobileModelName.ID,
	columns.CoreInterfaces.DeviceMobileModelName.Field,
	columns.WithEventColumnDocs(
		"Device Model (mobile)",
		deviceStubDescription,
	),
)

// DeviceOperatingSystemStubColumn is the column for the device operating system of an event
var DeviceOperatingSystemStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.DeviceOperatingSystem.ID,
	columns.CoreInterfaces.DeviceOperatingSystem.Field,
	columns.WithEventColumnDocs(
		"Operating System",
		deviceStubDescription,
	),
)

// DeviceOperatingSystemVersionStubColumn is the column for the device operating system version of an event
var DeviceOperatingSystemVersionStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.DeviceOperatingSystemVersion.ID,
	columns.CoreInterfaces.DeviceOperatingSystemVersion.Field,
	columns.WithEventColumnDocs(
		"Operating System Version",
		deviceStubDescription,
	),
)

// DeviceWebBrowserStubColumn is the column for the device web browser of an event
var DeviceWebBrowserStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.DeviceWebBrowser.ID,
	columns.CoreInterfaces.DeviceWebBrowser.Field,
	columns.WithEventColumnDocs(
		"Web Browser",
		deviceStubDescription,
	),
)

// DeviceWebBrowserVersionStubColumn is the column for the device web browser version of an event
var DeviceWebBrowserVersionStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.DeviceWebBrowserVersion.ID,
	columns.CoreInterfaces.DeviceWebBrowserVersion.Field,
	columns.WithEventColumnDocs(
		"Web Browser Version",
		deviceStubDescription,
	),
)
