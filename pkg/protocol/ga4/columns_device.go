package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/sirupsen/logrus"

	"github.com/mileusna/useragent"
)

const (
	DeviceCategoryMobile  = "mobile"
	DeviceCategoryTablet  = "tablet"
	DeviceCategoryDesktop = "desktop"
	DeviceCategoryBot     = "bot"
	DeviceCategoryUnknown = "unknown"

	DeviceBrandUnknown = "unknown"
	DeviceModelUnknown = "unknown"
)

/*
	DeviceCategory               schema.Interface
	DeviceMobileBrandName        schema.Interface
	DeviceMobileModelName        schema.Interface
	DeviceMobileMarketingName    schema.Interface
	DeviceMobileOSHardwareModel  schema.Interface
	DeviceOperatingSystem        schema.Interface
	DeviceOperatingSystemVersion schema.Interface
	DeviceLanguage               schema.Interface
	DeviceWebBrowser             schema.Interface
	DeviceWebBrowserVersion      schema.Interface
*/

func getUserAgent(event *schema.Event) (useragent.UserAgent, error) {
	ua, ok := event.Metadata["user_agent"]
	if ok {
		typedUA, ok := ua.(useragent.UserAgent)
		if ok {
			return typedUA, nil
		}
	}
	newUa := useragent.Parse(event.BoundHit.Headers.Get("User-Agent"))
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
				return DeviceCategoryMobile, nil
			}
		}

		ua, err := getUserAgent(event)
		if err != nil {
			logrus.Warnf(
				"deviceCategoryColumn: %s: %v",
				columns.CoreInterfaces.DeviceCategory.ID,
				err,
			)
			return DeviceCategoryUnknown, nil
		}
		if ua.Mobile {
			return DeviceCategoryMobile, nil
		}
		if ua.Tablet {
			return DeviceCategoryTablet, nil
		}
		if ua.Desktop {
			return DeviceCategoryDesktop, nil
		}
		if ua.Bot {
			return DeviceCategoryBot, nil
		}
		return DeviceCategoryUnknown, nil
	},
)

var deviceMobileBrandNameColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceMobileBrandName.ID,
	columns.CoreInterfaces.DeviceMobileBrandName.Field,
	func(event *schema.Event) (any, error) {

		ua, err := getUserAgent(event)
		if err != nil {
			logrus.Warnf(
				"deviceMobileBrandNameColumn: %s: %v",
				columns.CoreInterfaces.DeviceMobileBrandName.ID,
				err,
			)
			return DeviceBrandUnknown, nil
		}
		return ua.Name, nil
	},
)

var deviceMobileModelNameColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceMobileModelName.ID,
	columns.CoreInterfaces.DeviceMobileModelName.Field,
	func(event *schema.Event) (any, error) {

		paramV := event.BoundHit.QueryParams.Get("uab")
		if paramV != "" {
			return paramV, nil
		}
		ua, err := getUserAgent(event)
		if err != nil {
			logrus.Warnf(
				"deviceMobileModelNameColumn: %s: %v",
				columns.CoreInterfaces.DeviceMobileModelName.ID,
				err,
			)
			return DeviceModelUnknown, nil
		}
		return ua.Device, nil
	},
)
