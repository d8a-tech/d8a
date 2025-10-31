package ga4

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/sirupsen/logrus"
	"github.com/slipros/devicedetector"
)

// DeviceCategorySmartphone is a const value for core.d8a.tech/events/device_category for smartphone devices
const DeviceCategorySmartphone = "smartphone"

var dd *devicedetector.DeviceDetector = func() *devicedetector.DeviceDetector {
	dd, err := devicedetector.NewDeviceDetector()
	if err != nil {
		panic(fmt.Sprintf("Failed to create device detector: %v", err))
	}
	return dd
}()

func getDeviceInfo(event *schema.Event) (*devicedetector.DeviceInfo, error) {
	ua, ok := event.Metadata["user_agent"]
	if ok {
		typedUA, ok := ua.(*devicedetector.DeviceInfo)
		if ok {
			return typedUA, nil
		}
	}
	newUa := dd.Parse(event.BoundHit.Headers.Get("User-Agent"))
	event.Metadata["user_agent"] = newUa
	return newUa, nil
}

var deviceCategoryColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceCategory.ID,
	columns.CoreInterfaces.DeviceCategory.Field,
	func(event *schema.Event) (any, error) {
		paramV := event.BoundHit.QueryParams.Get("uamb")
		if paramV != "" {
			isMobile, err := util.StrToBool(paramV)
			if err == nil && isMobile {
				return DeviceCategorySmartphone, nil
			}
		}

		ua, err := getDeviceInfo(event)
		if err != nil {
			logrus.Warnf(
				"deviceCategoryColumn: %s: %v",
				columns.CoreInterfaces.DeviceCategory.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if ua != nil {
			return ua.Type, nil
		}
		return nil, nil // nolint:nilnil // nil is valid
	},
	columns.WithEventColumnDocs(
		"Device Category",
		"The type of device used to access the site, extracted from the User-Agent header or query parameters (e.g., 'smartphone', 'desktop', 'tablet', ...).", // nolint:lll // it's a description
	),
)

var deviceMobileBrandNameColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceMobileBrandName.ID,
	columns.CoreInterfaces.DeviceMobileBrandName.Field,
	"",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetBrandName(), nil
	},
	columns.WithEventColumnDocs(
		"Device Brand (mobile)",
		"The brand name of the mobile device, populated only for mobile devices, extracted from User-Agent header (e.g., 'Apple', 'Samsung', 'Google'). ", // nolint:lll // it's a description
	),
)

var deviceMobileModelNameColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceMobileModelName.ID,
	columns.CoreInterfaces.DeviceMobileModelName.Field,
	"uam",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.Model, nil
	},
	columns.WithEventColumnDocs(
		"Device Model (mobile)",
		"The model name of the mobile device, populated only for mobile devices, extracted from User-Agent header (e.g.,  'iPhone 13', 'Galaxy S21').", // nolint:lll // it's a description
	),
)

var deviceOperatingSystemColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceOperatingSystem.ID,
	columns.CoreInterfaces.DeviceOperatingSystem.Field,
	"uap",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetOs().Name, nil
	},
	columns.WithEventColumnDocs(
		"Operating System",
		"The operating system running on the user's device, extracted from the User-Agent header (e.g., 'iOS', 'Android', 'Windows', 'macOS', 'GNU/Linux').", // nolint:lll // it's a description
	),
)

var deviceOperatingSystemVersionColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceOperatingSystemVersion.ID,
	columns.CoreInterfaces.DeviceOperatingSystemVersion.Field,
	"uapv",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetOs().Version, nil
	},
	columns.WithEventColumnDocs(
		"OS Version",
		"The version of the operating system running on the user's device, extracted from the User-Agent header (e.g., '26.0.1', '18.7').", // nolint:lll // it's a description
	),
)

var deviceLanguageColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceLanguage.ID,
	columns.CoreInterfaces.DeviceLanguage.Field,
	func(event *schema.Event) (any, error) {
		paramV := event.BoundHit.QueryParams.Get("ul")
		if paramV != "" {
			return paramV, nil
		}
		acceptLanguage := event.BoundHit.Headers.Get("Accept-Language")
		if acceptLanguage != "" {
			return acceptLanguage, nil
		}
		return nil, nil // nolint:nilnil // nil is valid
	},
	columns.WithEventColumnDocs(
		"Device Language",
		"The language setting of the user's device, extracted from the User-Agent header or device information, based on ISO 639 standard for languages and ISO 3166 for country codes (e.g., 'en-us', 'en-gb', 'de-de').", // nolint:lll // it's a description
	),
)

var deviceWebBrowserColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceWebBrowser.ID,
	columns.CoreInterfaces.DeviceWebBrowser.Field,
	"",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetClient().Name, nil
	},
	columns.WithEventColumnDocs(
		"Web Browser",
		"The browser used to access the site, extracted from the User-Agent header (e.g., 'Chrome', 'Safari', 'Firefox', 'Mobile Safari').", // nolint:lll // it's a description
	),
)

var deviceWebBrowserVersionColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceWebBrowserVersion.ID,
	columns.CoreInterfaces.DeviceWebBrowserVersion.Field,
	"",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetClient().Version, nil
	},
	columns.WithEventColumnDocs(
		"Browser Version",
		"The version of the browser used to access the site, extracted from the User-Agent header (e.g., '141.0.0.0', '26.0.1').", // nolint:lll // it's a description
	),
)
