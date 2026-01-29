package ga4

import (
	"fmt"
	"strings"
	"time"

	"github.com/archbottle/dd2/pkg/clienthints"
	"github.com/archbottle/dd2/pkg/detector"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/sirupsen/logrus"
)

// DeviceCategorySmartphone is a const value for core.d8a.tech/events/device_category for smartphone devices
const DeviceCategorySmartphone = "smartphone"

type deviceParser interface {
	Parse(ua string, ch *clienthints.ClientHints) *detector.ParseResult
}

type cachingDeviceParser struct {
	dd    *detector.DeviceDetector
	cache *ristretto.Cache[string, *detector.ParseResult]
}

func (p *cachingDeviceParser) Parse(ua string, ch *clienthints.ClientHints) *detector.ParseResult {
	// Build cache key from UA + relevant client hint headers
	cacheKey := buildCacheKey(ua, ch)
	item, ok := p.cache.Get(cacheKey)
	if ok {
		return item
	}
	result := p.dd.Parse(ua, ch)
	p.cache.SetWithTTL(cacheKey, result, 1, time.Second*30)
	return result
}

// buildCacheKey creates a cache key from UA and client hints to ensure
// different hint combinations produce different cache entries
func buildCacheKey(ua string, ch *clienthints.ClientHints) string {
	if ch == nil {
		return ua
	}
	var parts []string
	parts = append(parts, ua)
	if ch.GetBrandList() != nil {
		parts = append(parts, fmt.Sprintf("brands:%v", ch.GetBrandList()))
	}
	if ch.IsMobile() {
		parts = append(parts, "mobile:1")
	}
	if platform := ch.GetOperatingSystem(); platform != "" {
		parts = append(parts, fmt.Sprintf("platform:%s", platform))
	}
	if platformVer := ch.GetOperatingSystemVersion(); platformVer != "" {
		parts = append(parts, fmt.Sprintf("platformVer:%s", platformVer))
	}
	if model := ch.GetModel(); model != "" {
		parts = append(parts, fmt.Sprintf("model:%s", model))
	}
	return strings.Join(parts, "|")
}

var dd = func() deviceParser {
	dd, err := detector.New()
	if err != nil {
		panic(fmt.Sprintf("Failed to create device detector: %v", err))
	}
	c, err := ristretto.NewCache(&ristretto.Config[string, *detector.ParseResult]{
		NumCounters: 100000,
		MaxCost:     10000,
		BufferItems: 64,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create device detector cache: %v", err))
	}
	return &cachingDeviceParser{
		dd:    dd,
		cache: c,
	}
}()

func getDeviceInfo(event *schema.Event) (*detector.ParseResult, error) {
	// Check cache in metadata (using new key to avoid confusion)
	cached, ok := event.Metadata["device_detector"]
	if ok {
		if result, ok := cached.(*detector.ParseResult); ok {
			return result, nil
		}
	}

	// Parse with dd2
	// Headers are already canonicalized by the receiver package
	headers := event.BoundHit.MustParsedRequest().Headers
	ua := headers.Get("User-Agent")
	ch := clienthints.New(headers)
	result := dd.Parse(ua, ch)
	event.Metadata["device_detector"] = result
	return result, nil
}

var deviceCategoryColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceCategory.ID,
	columns.CoreInterfaces.DeviceCategory.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		paramV := event.BoundHit.MustParsedRequest().QueryParams.Get("uamb")
		if paramV != "" {
			isMobile, err := util.StrToBool(paramV)
			if err == nil && isMobile {
				return DeviceCategorySmartphone, nil
			}
		}

		result, err := getDeviceInfo(event)
		if err != nil {
			logrus.Warnf(
				"deviceCategoryColumn: %s: %v",
				columns.CoreInterfaces.DeviceCategory.ID,
				err,
			)
			return nil, nil // nolint:nilnil // nil is valid
		}
		if result != nil {
			if result.IsDesktop() {
				return detector.DeviceTypeNames[detector.DeviceTypeDesktop], nil
			}
			deviceType := result.GetDevice()
			if deviceType != detector.DeviceTypeUnknown {
				if name, ok := detector.DeviceTypeNames[deviceType]; ok {
					return name, nil
				}
			}
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
	func(_ *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError) {
		brand := result.GetBrand()
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

var deviceMobileModelNameColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceMobileModelName.ID,
	columns.CoreInterfaces.DeviceMobileModelName.Field,
	"uam",
	func(_ *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError) {
		model := result.GetModel()
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

var deviceOperatingSystemColumn = ColumnFromRawQueryParamOrDeviceInfo(
	columns.CoreInterfaces.DeviceOperatingSystem.ID,
	columns.CoreInterfaces.DeviceOperatingSystem.Field,
	"uap",
	func(_ *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError) {
		os := result.GetOS()
		if os == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		if os.Name == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return os.Name, nil
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
	func(_ *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError) {
		os := result.GetOS()
		if os == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		if os.Version == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return os.Version, nil
	},
	columns.WithEventColumnDocs(
		"Operating System Version",
		"The version of the operating system running on the user's device, extracted from the User-Agent header (e.g., '26.0.1', '18.7').", // nolint:lll // it's a description
	),
)

var deviceLanguageColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.DeviceLanguage.ID,
	columns.CoreInterfaces.DeviceLanguage.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		paramV := event.BoundHit.MustParsedRequest().QueryParams.Get("ul")
		if paramV != "" {
			return paramV, nil
		}
		acceptLanguage := event.BoundHit.MustParsedRequest().Headers.Get("Accept-Language")
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
	func(_ *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError) {
		client := result.GetClient()
		if client == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		if client.Name == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return client.Name, nil
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
	func(_ *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError) {
		client := result.GetClient()
		if client == nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		if client.Version == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return client.Version, nil
	},
	columns.WithEventColumnDocs(
		"Web Browser Version",
		"The version of the browser used to access the site, extracted from the User-Agent header (e.g., '141.0.0.0', '26.0.1').", // nolint:lll // it's a description
	),
)
