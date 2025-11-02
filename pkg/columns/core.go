package columns

import (
	"reflect" // nolint:depguard // it's not speed-sensitive

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// CoreInterfaces are the core columns that are always present in the schema.
var CoreInterfaces = struct {
	EventID                 schema.Interface
	EventName               schema.Interface
	EventTimestampUTC       schema.Interface
	EventDateUTC            schema.Interface
	EventPageTitle          schema.Interface
	EventPageReferrer       schema.Interface
	EventPagePath           schema.Interface
	EventPageLocation       schema.Interface
	EventPageHostname       schema.Interface
	EventClientID           schema.Interface
	EventUserID             schema.Interface
	EventPropertyID         schema.Interface
	EventTrackingProtocol   schema.Interface
	EventPlatform           schema.Interface
	EventIPAddress          schema.Interface
	EventUtmMarketingTactic schema.Interface
	EventUtmSourcePlatform  schema.Interface
	EventUtmTerm            schema.Interface
	EventUtmContent         schema.Interface
	EventUtmSource          schema.Interface
	EventUtmMedium          schema.Interface
	EventUtmCampaign        schema.Interface
	EventUtmID              schema.Interface
	EventUtmCreativeFormat  schema.Interface
	SessionID               schema.Interface
	SessionDuration         schema.Interface
	SessionFirstEventTime   schema.Interface
	SessionLastEventTime    schema.Interface
	SessionTotalEvents      schema.Interface
	SessionReferer          schema.Interface
	SSESessionHitNumber     schema.Interface
	SSESessionPageNumber    schema.Interface
	SSEIsEntry              schema.Interface
	// Click ids
	EventClickIDGclid   schema.Interface
	EventClickIDDclid   schema.Interface
	EventClickIDSrsltid schema.Interface
	EventClickIDGbraid  schema.Interface
	EventClickIDWbraid  schema.Interface
	EventClickIDMsclkid schema.Interface

	// Device
	DeviceCategory               schema.Interface
	DeviceMobileBrandName        schema.Interface
	DeviceMobileModelName        schema.Interface
	DeviceOperatingSystem        schema.Interface
	DeviceOperatingSystemVersion schema.Interface
	DeviceLanguage               schema.Interface
	DeviceWebBrowser             schema.Interface
	DeviceWebBrowserVersion      schema.Interface

	// Geo
	GeoContinent    schema.Interface
	GeoCountry      schema.Interface
	GeoRegion       schema.Interface
	GeoCity         schema.Interface
	GeoSubContinent schema.Interface
	GeoMetro        schema.Interface
}{
	EventID: schema.Interface{
		ID:      "core.d8a.tech/events/id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "id", Type: arrow.BinaryTypes.String},
	},
	EventName: schema.Interface{
		ID:      "core.d8a.tech/events/name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
	},
	EventTimestampUTC: schema.Interface{
		ID:      "core.d8a.tech/events/timestamp_utc",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "timestamp_utc", Type: arrow.FixedWidthTypes.Timestamp_s},
	},
	EventDateUTC: schema.Interface{
		ID:      "core.d8a.tech/events/date_utc",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "date_utc", Type: arrow.FixedWidthTypes.Date32},
	},
	EventPageTitle: schema.Interface{
		ID:      "core.d8a.tech/events/page_title",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "page_title", Type: arrow.BinaryTypes.String},
	},
	EventPageReferrer: schema.Interface{
		ID:      "core.d8a.tech/events/page_referrer",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "page_referrer", Type: arrow.BinaryTypes.String},
	},
	EventPagePath: schema.Interface{
		ID:      "core.d8a.tech/events/page_path",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "page_path", Type: arrow.BinaryTypes.String},
	},
	EventPageLocation: schema.Interface{
		ID:      "core.d8a.tech/events/page_location",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "page_location", Type: arrow.BinaryTypes.String},
	},
	EventPageHostname: schema.Interface{
		ID:      "core.d8a.tech/events/page_hostname",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "page_hostname", Type: arrow.BinaryTypes.String},
	},
	EventClientID: schema.Interface{
		ID:      "core.d8a.tech/events/client_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "client_id", Type: arrow.BinaryTypes.String},
	},
	EventUserID: schema.Interface{
		ID:      "core.d8a.tech/events/user_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "user_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventTrackingProtocol: schema.Interface{
		ID:      "core.d8a.tech/events/tracking_protocol",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "tracking_protocol", Type: arrow.BinaryTypes.String},
	},
	EventPropertyID: schema.Interface{
		ID:      "core.d8a.tech/events/property_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "property_id", Type: arrow.BinaryTypes.String},
	},
	EventPlatform: schema.Interface{
		ID:      "core.d8a.tech/events/platform",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "platform", Type: arrow.BinaryTypes.String},
	},
	EventIPAddress: schema.Interface{
		ID:      "core.d8a.tech/events/ip_address",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ip_address", Type: arrow.BinaryTypes.String},
	},
	EventUtmMarketingTactic: schema.Interface{
		ID:      "core.d8a.tech/events/utm_marketing_tactic",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_marketing_tactic", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmSourcePlatform: schema.Interface{
		ID:      "core.d8a.tech/events/utm_source_platform",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_source_platform", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmTerm: schema.Interface{
		ID:      "core.d8a.tech/events/utm_term",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmContent: schema.Interface{
		ID:      "core.d8a.tech/events/utm_content",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_content", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmSource: schema.Interface{
		ID:      "core.d8a.tech/events/utm_source",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmMedium: schema.Interface{
		ID:      "core.d8a.tech/events/utm_medium",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_medium", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmCampaign: schema.Interface{
		ID:      "core.d8a.tech/events/utm_campaign",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_campaign", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmID: schema.Interface{
		ID:      "core.d8a.tech/events/utm_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmCreativeFormat: schema.Interface{
		ID:      "core.d8a.tech/events/utm_creative_format",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "utm_creative_format", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionID: schema.Interface{
		ID:      "core.d8a.tech/sessions/id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
	},
	SessionDuration: schema.Interface{
		ID:      "core.d8a.tech/sessions/duration",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_duration", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionFirstEventTime: schema.Interface{
		ID:      "core.d8a.tech/sessions/first_event_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_first_event_time", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	},
	SessionLastEventTime: schema.Interface{
		ID:      "core.d8a.tech/sessions/last_event_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_last_event_time", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	},
	SessionTotalEvents: schema.Interface{
		ID:      "core.d8a.tech/sessions/total_events",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_total_events", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionReferer: schema.Interface{
		ID:      "core.d8a.tech/sessions/referer",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_referer", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SSESessionHitNumber: schema.Interface{
		ID:      "core.d8a.tech/events/session_hit_number",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_hit_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SSESessionPageNumber: schema.Interface{
		ID:      "core.d8a.tech/events/session_page_number",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_page_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SSEIsEntry: schema.Interface{
		ID:      "core.d8a.tech/events/session_is_entry",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_is_entry", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// Click ids
	EventClickIDGclid: schema.Interface{
		ID:      "core.d8a.tech/events/click_id_gclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "click_id_gclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDDclid: schema.Interface{
		ID:      "core.d8a.tech/events/click_id_dclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "click_id_dclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDSrsltid: schema.Interface{
		ID:      "core.d8a.tech/events/click_id_srsltid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "click_id_srsltid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDGbraid: schema.Interface{
		ID:      "core.d8a.tech/events/click_id_gbraid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "click_id_gbraid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDWbraid: schema.Interface{
		ID:      "core.d8a.tech/events/click_id_wbraid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "click_id_wbraid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDMsclkid: schema.Interface{
		ID:      "core.d8a.tech/events/click_id_msclkid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "click_id_msclkid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Device
	DeviceCategory: schema.Interface{
		ID:      "core.d8a.tech/events/device_category",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_category", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceMobileBrandName: schema.Interface{
		ID:      "core.d8a.tech/events/device_mobile_brand_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_mobile_brand_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceMobileModelName: schema.Interface{
		ID:      "core.d8a.tech/events/device_mobile_model_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_mobile_model_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceOperatingSystem: schema.Interface{
		ID:      "core.d8a.tech/events/device_operating_system",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_operating_system", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceOperatingSystemVersion: schema.Interface{
		ID:      "core.d8a.tech/events/device_operating_system_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_operating_system_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceLanguage: schema.Interface{
		ID:      "core.d8a.tech/events/device_language",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_language", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceWebBrowser: schema.Interface{
		ID:      "core.d8a.tech/events/device_web_browser",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_web_browser", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceWebBrowserVersion: schema.Interface{
		ID:      "core.d8a.tech/events/device_web_browser_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "device_web_browser_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Geo
	GeoContinent: schema.Interface{
		ID:      "core.d8a.tech/events/geo_continent",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "geo_continent", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoCountry: schema.Interface{
		ID:      "core.d8a.tech/events/geo_country",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "geo_country", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoRegion: schema.Interface{
		ID:      "core.d8a.tech/events/geo_region",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "geo_region", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoCity: schema.Interface{
		ID:      "core.d8a.tech/events/geo_city",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "geo_city", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoSubContinent: schema.Interface{
		ID:      "core.d8a.tech/events/geo_sub_continent",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "geo_sub_continent", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoMetro: schema.Interface{
		ID:      "core.d8a.tech/events/geo_metro",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "geo_metro", Type: arrow.BinaryTypes.String, Nullable: true},
	},
}

// GetAllCoreColumns returns a slice of all core column interfaces for easy consumption.
func GetAllCoreColumns() []schema.Interface {
	var columns []schema.Interface

	v := reflect.ValueOf(CoreInterfaces)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Type() == reflect.TypeOf(schema.Interface{}) {
			if col, ok := field.Interface().(schema.Interface); ok {
				columns = append(columns, col)
			}
		}
	}

	return columns
}
