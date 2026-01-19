package columns

import (
	"reflect" // nolint:depguard // it's not speed-sensitive

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// CoreInterfaces are the core columns that are always present in the schema.
var CoreInterfaces = struct {
	EventID           schema.Interface
	EventName         schema.Interface
	EventPropertyID   schema.Interface
	EventPropertyName schema.Interface
	EventDateUTC      schema.Interface
	EventTimestampUTC schema.Interface
	EventClientID     schema.Interface
	EventUserID       schema.Interface
	EventIPAddress    schema.Interface
	EventPageLocation schema.Interface
	EventPageHostname schema.Interface
	EventPagePath     schema.Interface
	EventPageTitle    schema.Interface
	EventPageReferrer    schema.Interface
	EventIgnoreReferrer  schema.Interface
	EventPlatform        schema.Interface

	// Event UTM parameters
	EventUtmCampaign        schema.Interface
	EventUtmSource          schema.Interface
	EventUtmMedium          schema.Interface
	EventUtmContent         schema.Interface
	EventUtmTerm            schema.Interface
	EventUtmID              schema.Interface
	EventUtmSourcePlatform  schema.Interface
	EventUtmCreativeFormat  schema.Interface
	EventUtmMarketingTactic schema.Interface

	// Click ids
	EventClickIDGclid   schema.Interface
	EventClickIDDclid   schema.Interface
	EventClickIDGbraid  schema.Interface
	EventClickIDSrsltid schema.Interface
	EventClickIDWbraid  schema.Interface
	EventClickIDFbclid  schema.Interface
	EventClickIDMsclkid schema.Interface

	// Geo
	GeoCity         schema.Interface
	GeoRegion       schema.Interface
	GeoMetro        schema.Interface
	GeoCountry      schema.Interface
	GeoContinent    schema.Interface
	GeoSubContinent schema.Interface

	// Device
	DeviceCategory               schema.Interface
	DeviceLanguage               schema.Interface
	DeviceMobileBrandName        schema.Interface
	DeviceMobileModelName        schema.Interface
	DeviceOperatingSystem        schema.Interface
	DeviceOperatingSystemVersion schema.Interface
	DeviceWebBrowser             schema.Interface
	DeviceWebBrowserVersion      schema.Interface

	EventTrackingProtocol schema.Interface

	// Session-scoped event columns
	SSEIsEntry           schema.Interface
	SSESessionHitNumber  schema.Interface
	SSESessionPageNumber schema.Interface
	SSETimeOnPage        schema.Interface
	SSEIsEntryPage       schema.Interface
	SSEIsExitPage        schema.Interface

	// Session columns
	SessionID schema.Interface

	SessionSource schema.Interface
	SessionMedium schema.Interface
	SessionTerm   schema.Interface

	SessionReferrer       schema.Interface
	SessionDuration       schema.Interface
	SessionTotalEvents    schema.Interface
	SessionFirstEventTime schema.Interface
	SessionLastEventTime  schema.Interface

	SessionEntryPageLocation  schema.Interface
	SessionSecondPageLocation schema.Interface
	SessionExitPageLocation   schema.Interface
	SessionEntryPageTitle     schema.Interface
	SessionSecondPageTitle    schema.Interface
	SessionExitPageTitle      schema.Interface

	// Session UTM parameters
	SessionUtmCampaign        schema.Interface
	SessionUtmSource          schema.Interface
	SessionUtmMedium          schema.Interface
	SessionUtmContent         schema.Interface
	SessionUtmTerm            schema.Interface
	SessionUtmID              schema.Interface
	SessionUtmSourcePlatform  schema.Interface
	SessionUtmCreativeFormat  schema.Interface
	SessionUtmMarketingTactic schema.Interface

	// Session Click IDs
	SessionClickIDGclid   schema.Interface
	SessionClickIDDclid   schema.Interface
	SessionClickIDGbraid  schema.Interface
	SessionClickIDSrsltid schema.Interface
	SessionClickIDWbraid  schema.Interface
	SessionClickIDFbclid  schema.Interface
	SessionClickIDMsclkid schema.Interface

	// Totals
	SessionTotalPageViews         schema.Interface
	SessionUniquePageViews        schema.Interface
	SessionTotalPurchases         schema.Interface
	SessionTotalScrolls           schema.Interface
	SessionTotalOutboundClicks    schema.Interface
	SessionUniqueOutboundClicks   schema.Interface
	SessionTotalSiteSearches      schema.Interface
	SessionUniqueSiteSearches     schema.Interface
	SessionTotalFormInteractions  schema.Interface
	SessionUniqueFormInteractions schema.Interface
	SessionTotalVideoEngagements  schema.Interface
	SessionTotalFileDownloads     schema.Interface
	SessionUniqueFileDownloads    schema.Interface

	SessionSplitCause schema.Interface
}{
	EventID: schema.Interface{
		ID:    "core.d8a.tech/events/id",
		Field: &arrow.Field{Name: "id", Type: arrow.BinaryTypes.String},
	},
	EventName: schema.Interface{
		ID:    "core.d8a.tech/events/name",
		Field: &arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
	},
	EventPropertyID: schema.Interface{
		ID:    "core.d8a.tech/events/property_id",
		Field: &arrow.Field{Name: "property_id", Type: arrow.BinaryTypes.String},
	},
	EventPropertyName: schema.Interface{
		ID:    "core.d8a.tech/events/property_name",
		Field: &arrow.Field{Name: "property_name", Type: arrow.BinaryTypes.String},
	},
	EventDateUTC: schema.Interface{
		ID:    "core.d8a.tech/events/date_utc",
		Field: &arrow.Field{Name: "date_utc", Type: arrow.FixedWidthTypes.Date32},
	},
	EventTimestampUTC: schema.Interface{
		ID:    "core.d8a.tech/events/timestamp_utc",
		Field: &arrow.Field{Name: "timestamp_utc", Type: arrow.FixedWidthTypes.Timestamp_s},
	},
	EventClientID: schema.Interface{
		ID:    "core.d8a.tech/events/client_id",
		Field: &arrow.Field{Name: "client_id", Type: arrow.BinaryTypes.String},
	},
	EventUserID: schema.Interface{
		ID:    "core.d8a.tech/events/user_id",
		Field: &arrow.Field{Name: "user_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventIPAddress: schema.Interface{
		ID:    "core.d8a.tech/events/ip_address",
		Field: &arrow.Field{Name: "ip_address", Type: arrow.BinaryTypes.String},
	},
	EventPageLocation: schema.Interface{
		ID:    "core.d8a.tech/events/page_location",
		Field: &arrow.Field{Name: "page_location", Type: arrow.BinaryTypes.String},
	},
	EventPageHostname: schema.Interface{
		ID:    "core.d8a.tech/events/page_hostname",
		Field: &arrow.Field{Name: "page_hostname", Type: arrow.BinaryTypes.String},
	},
	EventPagePath: schema.Interface{
		ID:    "core.d8a.tech/events/page_path",
		Field: &arrow.Field{Name: "page_path", Type: arrow.BinaryTypes.String},
	},
	EventPageTitle: schema.Interface{
		ID:    "core.d8a.tech/events/page_title",
		Field: &arrow.Field{Name: "page_title", Type: arrow.BinaryTypes.String},
	},
	EventPageReferrer: schema.Interface{
		ID:    "core.d8a.tech/events/page_referrer",
		Field: &arrow.Field{Name: "page_referrer", Type: arrow.BinaryTypes.String},
	},
	EventIgnoreReferrer: schema.Interface{
		ID:    "core.d8a.tech/events/ignore_referrer",
		Field: &arrow.Field{Name: "ignore_referrer", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventPlatform: schema.Interface{
		ID:    "core.d8a.tech/events/platform",
		Field: &arrow.Field{Name: "platform", Type: arrow.BinaryTypes.String},
	},
	EventUtmCampaign: schema.Interface{
		ID:    "core.d8a.tech/events/utm_campaign",
		Field: &arrow.Field{Name: "utm_campaign", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmSource: schema.Interface{
		ID:    "core.d8a.tech/events/utm_source",
		Field: &arrow.Field{Name: "utm_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmMedium: schema.Interface{
		ID:    "core.d8a.tech/events/utm_medium",
		Field: &arrow.Field{Name: "utm_medium", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmContent: schema.Interface{
		ID:    "core.d8a.tech/events/utm_content",
		Field: &arrow.Field{Name: "utm_content", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmTerm: schema.Interface{
		ID:    "core.d8a.tech/events/utm_term",
		Field: &arrow.Field{Name: "utm_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmID: schema.Interface{
		ID:    "core.d8a.tech/events/utm_id",
		Field: &arrow.Field{Name: "utm_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmSourcePlatform: schema.Interface{
		ID:    "core.d8a.tech/events/utm_source_platform",
		Field: &arrow.Field{Name: "utm_source_platform", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmCreativeFormat: schema.Interface{
		ID:    "core.d8a.tech/events/utm_creative_format",
		Field: &arrow.Field{Name: "utm_creative_format", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUtmMarketingTactic: schema.Interface{
		ID:    "core.d8a.tech/events/utm_marketing_tactic",
		Field: &arrow.Field{Name: "utm_marketing_tactic", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDGclid: schema.Interface{
		ID:    "core.d8a.tech/events/click_id_gclid",
		Field: &arrow.Field{Name: "click_id_gclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDDclid: schema.Interface{
		ID:    "core.d8a.tech/events/click_id_dclid",
		Field: &arrow.Field{Name: "click_id_dclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDGbraid: schema.Interface{
		ID:    "core.d8a.tech/events/click_id_gbraid",
		Field: &arrow.Field{Name: "click_id_gbraid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDSrsltid: schema.Interface{
		ID:    "core.d8a.tech/events/click_id_srsltid",
		Field: &arrow.Field{Name: "click_id_srsltid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDWbraid: schema.Interface{
		ID:    "core.d8a.tech/events/click_id_wbraid",
		Field: &arrow.Field{Name: "click_id_wbraid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDFbclid: schema.Interface{
		ID:    "core.d8a.tech/events/click_id_fbclid",
		Field: &arrow.Field{Name: "click_id_fbclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventClickIDMsclkid: schema.Interface{
		ID:    "core.d8a.tech/events/click_id_msclkid",
		Field: &arrow.Field{Name: "click_id_msclkid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoCity: schema.Interface{
		ID:    "core.d8a.tech/events/geo_city",
		Field: &arrow.Field{Name: "geo_city", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoRegion: schema.Interface{
		ID:    "core.d8a.tech/events/geo_region",
		Field: &arrow.Field{Name: "geo_region", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoMetro: schema.Interface{
		ID:    "core.d8a.tech/events/geo_metro",
		Field: &arrow.Field{Name: "geo_metro", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoCountry: schema.Interface{
		ID:    "core.d8a.tech/events/geo_country",
		Field: &arrow.Field{Name: "geo_country", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoContinent: schema.Interface{
		ID:    "core.d8a.tech/events/geo_continent",
		Field: &arrow.Field{Name: "geo_continent", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GeoSubContinent: schema.Interface{
		ID:    "core.d8a.tech/events/geo_sub_continent",
		Field: &arrow.Field{Name: "geo_sub_continent", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceCategory: schema.Interface{
		ID:    "core.d8a.tech/events/device_category",
		Field: &arrow.Field{Name: "device_category", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceLanguage: schema.Interface{
		ID:    "core.d8a.tech/events/device_language",
		Field: &arrow.Field{Name: "device_language", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceMobileBrandName: schema.Interface{
		ID:    "core.d8a.tech/events/device_mobile_brand_name",
		Field: &arrow.Field{Name: "device_mobile_brand_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceMobileModelName: schema.Interface{
		ID:    "core.d8a.tech/events/device_mobile_model_name",
		Field: &arrow.Field{Name: "device_mobile_model_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceOperatingSystem: schema.Interface{
		ID:    "core.d8a.tech/events/device_operating_system",
		Field: &arrow.Field{Name: "device_operating_system", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceOperatingSystemVersion: schema.Interface{
		ID:    "core.d8a.tech/events/device_operating_system_version",
		Field: &arrow.Field{Name: "device_operating_system_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceWebBrowser: schema.Interface{
		ID:    "core.d8a.tech/events/device_web_browser",
		Field: &arrow.Field{Name: "device_web_browser", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	DeviceWebBrowserVersion: schema.Interface{
		ID:    "core.d8a.tech/events/device_web_browser_version",
		Field: &arrow.Field{Name: "device_web_browser_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventTrackingProtocol: schema.Interface{
		ID:    "core.d8a.tech/events/tracking_protocol",
		Field: &arrow.Field{Name: "tracking_protocol", Type: arrow.BinaryTypes.String},
	},
	SSEIsEntry: schema.Interface{
		ID:    "core.d8a.tech/events/session_is_entry",
		Field: &arrow.Field{Name: "session_is_entry", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SSESessionHitNumber: schema.Interface{
		ID:    "core.d8a.tech/events/session_hit_number",
		Field: &arrow.Field{Name: "session_hit_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SSESessionPageNumber: schema.Interface{
		ID:    "core.d8a.tech/events/session_page_number",
		Field: &arrow.Field{Name: "session_page_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SSETimeOnPage: schema.Interface{
		ID:    "core.d8a.tech/events/time_on_page",
		Field: &arrow.Field{Name: "time_on_page", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SSEIsEntryPage: schema.Interface{
		ID:    "core.d8a.tech/events/session_is_entry_page",
		Field: &arrow.Field{Name: "session_is_entry_page", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SSEIsExitPage: schema.Interface{
		ID:    "core.d8a.tech/events/session_is_exit_page",
		Field: &arrow.Field{Name: "session_is_exit_page", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},

	SessionID: schema.Interface{
		ID:    "core.d8a.tech/sessions/id",
		Field: &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
	},
	SessionSource: schema.Interface{
		ID:    "core.d8a.tech/sessions/source",
		Field: &arrow.Field{Name: "session_source", Type: arrow.BinaryTypes.String},
	},
	SessionMedium: schema.Interface{
		ID:    "core.d8a.tech/sessions/medium",
		Field: &arrow.Field{Name: "session_medium", Type: arrow.BinaryTypes.String},
	},
	SessionTerm: schema.Interface{
		ID:    "core.d8a.tech/sessions/term",
		Field: &arrow.Field{Name: "session_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionReferrer: schema.Interface{
		ID:    "core.d8a.tech/sessions/referrer",
		Field: &arrow.Field{Name: "session_referrer", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionDuration: schema.Interface{
		ID:    "core.d8a.tech/sessions/duration",
		Field: &arrow.Field{Name: "session_duration", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalEvents: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_events",
		Field: &arrow.Field{Name: "session_total_events", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionFirstEventTime: schema.Interface{
		ID:    "core.d8a.tech/sessions/first_event_time",
		Field: &arrow.Field{Name: "session_first_event_time", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	},
	SessionLastEventTime: schema.Interface{
		ID:    "core.d8a.tech/sessions/last_event_time",
		Field: &arrow.Field{Name: "session_last_event_time", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	},
	SessionEntryPageLocation: schema.Interface{
		ID:    "core.d8a.tech/sessions/entry_page_location",
		Field: &arrow.Field{Name: "session_entry_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionSecondPageLocation: schema.Interface{
		ID:    "core.d8a.tech/sessions/second_page_location",
		Field: &arrow.Field{Name: "session_second_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionExitPageLocation: schema.Interface{
		ID:    "core.d8a.tech/sessions/exit_page_location",
		Field: &arrow.Field{Name: "session_exit_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionEntryPageTitle: schema.Interface{
		ID:    "core.d8a.tech/sessions/entry_page_title",
		Field: &arrow.Field{Name: "session_entry_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionSecondPageTitle: schema.Interface{
		ID:    "core.d8a.tech/sessions/second_page_title",
		Field: &arrow.Field{Name: "session_second_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionExitPageTitle: schema.Interface{
		ID:    "core.d8a.tech/sessions/exit_page_title",
		Field: &arrow.Field{Name: "session_exit_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmCampaign: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_campaign",
		Field: &arrow.Field{Name: "session_utm_campaign", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmSource: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_source",
		Field: &arrow.Field{Name: "session_utm_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmMedium: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_medium",
		Field: &arrow.Field{Name: "session_utm_medium", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmContent: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_content",
		Field: &arrow.Field{Name: "session_utm_content", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmTerm: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_term",
		Field: &arrow.Field{Name: "session_utm_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmID: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_id",
		Field: &arrow.Field{Name: "session_utm_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmSourcePlatform: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_source_platform",
		Field: &arrow.Field{Name: "session_utm_source_platform", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmCreativeFormat: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_creative_format",
		Field: &arrow.Field{Name: "session_utm_creative_format", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionUtmMarketingTactic: schema.Interface{
		ID:    "core.d8a.tech/sessions/utm_marketing_tactic",
		Field: &arrow.Field{Name: "session_utm_marketing_tactic", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionClickIDGclid: schema.Interface{
		ID:    "core.d8a.tech/sessions/click_id_gclid",
		Field: &arrow.Field{Name: "session_click_id_gclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionClickIDDclid: schema.Interface{
		ID:    "core.d8a.tech/sessions/click_id_dclid",
		Field: &arrow.Field{Name: "session_click_id_dclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionClickIDGbraid: schema.Interface{
		ID:    "core.d8a.tech/sessions/click_id_gbraid",
		Field: &arrow.Field{Name: "session_click_id_gbraid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionClickIDSrsltid: schema.Interface{
		ID:    "core.d8a.tech/sessions/click_id_srsltid",
		Field: &arrow.Field{Name: "session_click_id_srsltid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionClickIDWbraid: schema.Interface{
		ID:    "core.d8a.tech/sessions/click_id_wbraid",
		Field: &arrow.Field{Name: "session_click_id_wbraid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionClickIDFbclid: schema.Interface{
		ID:    "core.d8a.tech/sessions/click_id_fbclid",
		Field: &arrow.Field{Name: "session_click_id_fbclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionClickIDMsclkid: schema.Interface{
		ID:    "core.d8a.tech/sessions/click_id_msclkid",
		Field: &arrow.Field{Name: "session_click_id_msclkid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionTotalPageViews: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_page_views",
		Field: &arrow.Field{Name: "session_total_page_views", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionUniquePageViews: schema.Interface{
		ID:    "core.d8a.tech/sessions/unique_page_views",
		Field: &arrow.Field{Name: "session_unique_page_views", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalPurchases: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_purchases",
		Field: &arrow.Field{Name: "session_total_purchases", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalScrolls: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_scrolls",
		Field: &arrow.Field{Name: "session_total_scrolls", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalOutboundClicks: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_outbound_clicks",
		Field: &arrow.Field{Name: "session_total_outbound_clicks", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionUniqueOutboundClicks: schema.Interface{
		ID:    "core.d8a.tech/sessions/unique_outbound_clicks",
		Field: &arrow.Field{Name: "session_unique_outbound_clicks", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalSiteSearches: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_site_searches",
		Field: &arrow.Field{Name: "session_total_site_searches", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionUniqueSiteSearches: schema.Interface{
		ID:    "core.d8a.tech/sessions/unique_site_searches",
		Field: &arrow.Field{Name: "session_unique_site_searches", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalFormInteractions: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_form_interactions",
		Field: &arrow.Field{Name: "session_total_form_interactions", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionUniqueFormInteractions: schema.Interface{
		ID:    "core.d8a.tech/sessions/unique_form_interactions",
		Field: &arrow.Field{Name: "session_unique_form_interactions", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalVideoEngagements: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_video_engagements",
		Field: &arrow.Field{Name: "session_total_video_engagements", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalFileDownloads: schema.Interface{
		ID:    "core.d8a.tech/sessions/total_file_downloads",
		Field: &arrow.Field{Name: "session_total_file_downloads", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionUniqueFileDownloads: schema.Interface{
		ID:    "core.d8a.tech/sessions/unique_file_downloads",
		Field: &arrow.Field{Name: "session_unique_file_downloads", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionSplitCause: schema.Interface{
		ID:    "core.d8a.tech/sessions/split_cause",
		Field: &arrow.Field{Name: "session_split_cause", Type: arrow.BinaryTypes.String, Nullable: true},
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
