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
)

var deviceMobileBrandNameColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceMobileBrandName.ID,
	columns.CoreInterfaces.DeviceMobileBrandName.Field,
	"",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.Brand, nil
	},
)

var deviceMobileModelNameColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceMobileModelName.ID,
	columns.CoreInterfaces.DeviceMobileModelName.Field,
	"uam",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.Model, nil
	},
)

var deviceOperatingSystemColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceOperatingSystem.ID,
	columns.CoreInterfaces.DeviceOperatingSystem.Field,
	"uap",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetOs().Name, nil
	},
)

var deviceOperatingSystemVersionColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceOperatingSystemVersion.ID,
	columns.CoreInterfaces.DeviceOperatingSystemVersion.Field,
	"uapv",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetOs().Version, nil
	},
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
)

var deviceWebBrowserColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceWebBrowser.ID,
	columns.CoreInterfaces.DeviceWebBrowser.Field,
	"",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetClient().Name, nil
	},
)

var deviceWebBrowserVersionColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceWebBrowserVersion.ID,
	columns.CoreInterfaces.DeviceWebBrowserVersion.Field,
	"",
	func(_ *schema.Event, di *devicedetector.DeviceInfo) (any, error) {
		return di.GetClient().Version, nil
	},
)
