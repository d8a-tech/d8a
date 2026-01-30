// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/archbottle/dd2/pkg/detector"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

func deviceColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	getValue func(*detector.FullInfo) (any, schema.D8AColumnWriteError),
	opts ...columns.EventColumnOptions,
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		id,
		field,
		func(e *schema.Event) (any, schema.D8AColumnWriteError) {
			info, err := columns.DeviceFullInfo(e)
			if err != nil || info == nil {
				return nil, schema.NewBrokenEventError(
					fmt.Sprintf("failed to get device full info: %v", err),
				)
			}
			return getValue(info)
		},
		opts...,
	)
}

// DeviceCategoryColumn is the column for the device category of an event
var DeviceCategoryColumn = deviceColumn(
	columns.CoreInterfaces.DeviceCategory.ID,
	columns.CoreInterfaces.DeviceCategory.Field,
	func(info *detector.FullInfo) (any, schema.D8AColumnWriteError) {
		if info.Device == nil || info.Device.Type == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return info.Device.Type, nil
	},
	columns.WithEventColumnDocs(
		"Device Category",
		"The type of device used to access the site, extracted from the User-Agent header (e.g., 'smartphone', 'desktop', 'tablet', ...).", // nolint:lll // it's a description
	),
)

// DeviceMobileBrandNameColumn is the column for the device mobile brand name of an event
var DeviceMobileBrandNameColumn = deviceColumn(
	columns.CoreInterfaces.DeviceMobileBrandName.ID,
	columns.CoreInterfaces.DeviceMobileBrandName.Field,
	func(info *detector.FullInfo) (any, schema.D8AColumnWriteError) {
		if info.Device == nil || info.Device.Brand == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return info.Device.Brand, nil
	},
	columns.WithEventColumnDocs(
		"Device Brand (mobile)",
		"The brand name of the mobile device, populated only for mobile devices, extracted from User-Agent header (e.g., 'Apple', 'Samsung', 'Google'). ", // nolint:lll // it's a description
	),
)

// DeviceMobileModelNameColumn is the column for the device mobile model name of an event
var DeviceMobileModelNameColumn = deviceColumn(
	columns.CoreInterfaces.DeviceMobileModelName.ID,
	columns.CoreInterfaces.DeviceMobileModelName.Field,
	func(info *detector.FullInfo) (any, schema.D8AColumnWriteError) {
		if info.Device == nil || info.Device.Model == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return info.Device.Model, nil
	},
	columns.WithEventColumnDocs(
		"Device Model (mobile)",
		"The model name of the mobile device, populated only for mobile devices, extracted from User-Agent header (e.g.,  'iPhone 13', 'Galaxy S21').", // nolint:lll // it's a description
	),
)

// DeviceOperatingSystemColumn is the column for the device operating system of an event
var DeviceOperatingSystemColumn = deviceColumn(
	columns.CoreInterfaces.DeviceOperatingSystem.ID,
	columns.CoreInterfaces.DeviceOperatingSystem.Field,
	func(info *detector.FullInfo) (any, schema.D8AColumnWriteError) {
		if info.OS == nil || info.OS.Name == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return info.OS.Name, nil
	},
	columns.WithEventColumnDocs(
		"Operating System",
		"The operating system running on the user's device, extracted from the User-Agent header (e.g., 'iOS', 'Android', 'Windows', 'macOS', 'GNU/Linux').", // nolint:lll // it's a description
	),
)

// DeviceOperatingSystemVersionColumn is the column for the device operating system version of an event
var DeviceOperatingSystemVersionColumn = deviceColumn(
	columns.CoreInterfaces.DeviceOperatingSystemVersion.ID,
	columns.CoreInterfaces.DeviceOperatingSystemVersion.Field,
	func(info *detector.FullInfo) (any, schema.D8AColumnWriteError) {
		if info.OS == nil || info.OS.Version == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return info.OS.Version, nil
	},
	columns.WithEventColumnDocs(
		"Operating System Version",
		"The version of the operating system running on the user's device, extracted from the User-Agent header (e.g., '26.0.1', '18.7').", // nolint:lll // it's a description
	),
)

// DeviceWebBrowserColumn is the column for the device web browser of an event
var DeviceWebBrowserColumn = deviceColumn(
	columns.CoreInterfaces.DeviceWebBrowser.ID,
	columns.CoreInterfaces.DeviceWebBrowser.Field,
	func(info *detector.FullInfo) (any, schema.D8AColumnWriteError) {
		if info.Client == nil || info.Client.Name == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return info.Client.Name, nil
	},
	columns.WithEventColumnDocs(
		"Web Browser",
		"The browser used to access the site, extracted from the User-Agent header (e.g., 'Chrome', 'Safari', 'Firefox', 'Mobile Safari').", // nolint:lll // it's a description
	),
)

// DeviceWebBrowserVersionColumn is the column for the device web browser version of an event
var DeviceWebBrowserVersionColumn = deviceColumn(
	columns.CoreInterfaces.DeviceWebBrowserVersion.ID,
	columns.CoreInterfaces.DeviceWebBrowserVersion.Field,
	func(info *detector.FullInfo) (any, schema.D8AColumnWriteError) {
		if info.Client == nil || info.Client.Version == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return info.Client.Version, nil
	},
	columns.WithEventColumnDocs(
		"Web Browser Version",
		"The version of the browser used to access the site, extracted from the User-Agent header (e.g., '141.0.0.0', '26.0.1').", // nolint:lll // it's a description
	),
)
