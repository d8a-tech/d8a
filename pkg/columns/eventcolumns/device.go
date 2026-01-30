// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

// DeviceCategoryColumn is the column for the device category of an event
var DeviceCategoryColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceCategory.ID,
	columns.CoreInterfaces.DeviceCategory.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		info, err := columns.DeviceFullInfo(event)
		if err != nil {
			logrus.Warnf(
				"DeviceCategoryColumn: %s: %v",
				columns.CoreInterfaces.DeviceCategory.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if info == nil || info.Device == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		deviceType := info.Device.Type
		if deviceType == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return deviceType, nil
	},
	columns.WithEventColumnDocs(
		"Device Category",
		"The type of device used to access the site, extracted from the User-Agent header (e.g., 'smartphone', 'desktop', 'tablet', ...).", // nolint:lll // it's a description
	),
)

// DeviceMobileBrandNameColumn is the column for the device mobile brand name of an event
var DeviceMobileBrandNameColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceMobileBrandName.ID,
	columns.CoreInterfaces.DeviceMobileBrandName.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		info, err := columns.DeviceFullInfo(event)
		if err != nil {
			logrus.Warnf(
				"DeviceMobileBrandNameColumn: %s: %v",
				columns.CoreInterfaces.DeviceMobileBrandName.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if info == nil || info.Device == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		brand := info.Device.Brand
		if brand == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return brand, nil
	},
	columns.WithEventColumnDocs(
		"Device Brand (mobile)",
		"The brand name of the mobile device, populated only for mobile devices, extracted from User-Agent header (e.g., 'Apple', 'Samsung', 'Google'). ", // nolint:lll // it's a description
	),
)

// DeviceMobileModelNameColumn is the column for the device mobile model name of an event
var DeviceMobileModelNameColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceMobileModelName.ID,
	columns.CoreInterfaces.DeviceMobileModelName.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		info, err := columns.DeviceFullInfo(event)
		if err != nil {
			logrus.Warnf(
				"DeviceMobileModelNameColumn: %s: %v",
				columns.CoreInterfaces.DeviceMobileModelName.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if info == nil || info.Device == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		model := info.Device.Model
		if model == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return model, nil
	},
	columns.WithEventColumnDocs(
		"Device Model (mobile)",
		"The model name of the mobile device, populated only for mobile devices, extracted from User-Agent header (e.g.,  'iPhone 13', 'Galaxy S21').", // nolint:lll // it's a description
	),
)

// DeviceOperatingSystemColumn is the column for the device operating system of an event
var DeviceOperatingSystemColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceOperatingSystem.ID,
	columns.CoreInterfaces.DeviceOperatingSystem.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		info, err := columns.DeviceFullInfo(event)
		if err != nil {
			logrus.Warnf(
				"DeviceOperatingSystemColumn: %s: %v",
				columns.CoreInterfaces.DeviceOperatingSystem.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if info == nil || info.OS == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		osName := info.OS.Name
		if osName == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return osName, nil
	},
	columns.WithEventColumnDocs(
		"Operating System",
		"The operating system running on the user's device, extracted from the User-Agent header (e.g., 'iOS', 'Android', 'Windows', 'macOS', 'GNU/Linux').", // nolint:lll // it's a description
	),
)

// DeviceOperatingSystemVersionColumn is the column for the device operating system version of an event
var DeviceOperatingSystemVersionColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceOperatingSystemVersion.ID,
	columns.CoreInterfaces.DeviceOperatingSystemVersion.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		info, err := columns.DeviceFullInfo(event)
		if err != nil {
			logrus.Warnf(
				"DeviceOperatingSystemVersionColumn: %s: %v",
				columns.CoreInterfaces.DeviceOperatingSystemVersion.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if info == nil || info.OS == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		osVersion := info.OS.Version
		if osVersion == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return osVersion, nil
	},
	columns.WithEventColumnDocs(
		"Operating System Version",
		"The version of the operating system running on the user's device, extracted from the User-Agent header (e.g., '26.0.1', '18.7').", // nolint:lll // it's a description
	),
)

// DeviceWebBrowserColumn is the column for the device web browser of an event
var DeviceWebBrowserColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceWebBrowser.ID,
	columns.CoreInterfaces.DeviceWebBrowser.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		info, err := columns.DeviceFullInfo(event)
		if err != nil {
			logrus.Warnf(
				"DeviceWebBrowserColumn: %s: %v",
				columns.CoreInterfaces.DeviceWebBrowser.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if info == nil || info.Client == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		browserName := info.Client.Name
		if browserName == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return browserName, nil
	},
	columns.WithEventColumnDocs(
		"Web Browser",
		"The browser used to access the site, extracted from the User-Agent header (e.g., 'Chrome', 'Safari', 'Firefox', 'Mobile Safari').", // nolint:lll // it's a description
	),
)

// DeviceWebBrowserVersionColumn is the column for the device web browser version of an event
var DeviceWebBrowserVersionColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceWebBrowserVersion.ID,
	columns.CoreInterfaces.DeviceWebBrowserVersion.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		info, err := columns.DeviceFullInfo(event)
		if err != nil {
			logrus.Warnf(
				"DeviceWebBrowserVersionColumn: %s: %v",
				columns.CoreInterfaces.DeviceWebBrowserVersion.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if info == nil || info.Client == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		browserVersion := info.Client.Version
		if browserVersion == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return browserVersion, nil
	},
	columns.WithEventColumnDocs(
		"Web Browser Version",
		"The version of the browser used to access the site, extracted from the User-Agent header (e.g., '141.0.0.0', '26.0.1').", // nolint:lll // it's a description
	),
)
