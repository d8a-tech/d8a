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
	EventTimestamp          schema.Interface
	EventDate               schema.Interface
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
	EventGclid              schema.Interface
	EventDclid              schema.Interface
	EventSrsltid            schema.Interface
	EventAclid              schema.Interface
	EventAnid               schema.Interface
	EventUtmMarketingTactic schema.Interface
	EventUtmSourcePlatform  schema.Interface
	EventUtmTerm            schema.Interface
	EventUtmContent         schema.Interface
	EventUtmSource          schema.Interface
	EventUtmMedium          schema.Interface
	EventUtmCampaign        schema.Interface
	EventUtmId              schema.Interface
	EventUtmCreativeFormat  schema.Interface
	SessionID               schema.Interface
	SessionDuration         schema.Interface
	SessionFirstEventTime   schema.Interface
	SessionLastEventTime    schema.Interface
	SessionTotalEvents      schema.Interface
}{
	EventID: schema.Interface{
		ID:      "core.d8a.tech/events/id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
	},
	EventName: schema.Interface{
		ID:      "core.d8a.tech/events/name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "event_name", Type: arrow.BinaryTypes.String},
	},
	EventTimestamp: schema.Interface{
		ID:      "core.d8a.tech/events/timestamp",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
	},
	EventDate: schema.Interface{
		ID:      "core.d8a.tech/events/date",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "event_date", Type: arrow.FixedWidthTypes.Date32},
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
	EventGclid: schema.Interface{
		ID:      "core.d8a.tech/events/gclid",
		Version: "1.0.0",
		Field: &arrow.Field{
			Name:     "params_gclid",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
	},
	EventDclid: schema.Interface{
		ID:      "core.d8a.tech/events/dclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_dclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSrsltid: schema.Interface{
		ID:      "core.d8a.tech/events/srsltid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_srsltid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventAclid: schema.Interface{
		ID:      "core.d8a.tech/events/aclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_aclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventAnid: schema.Interface{
		ID:      "core.d8a.tech/events/anid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_anid", Type: arrow.BinaryTypes.String, Nullable: true},
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
	EventUtmId: schema.Interface{
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
		Field:   &arrow.Field{Name: "duration", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionFirstEventTime: schema.Interface{
		ID:      "core.d8a.tech/sessions/first_event_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "first_event_time", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	},
	SessionLastEventTime: schema.Interface{
		ID:      "core.d8a.tech/sessions/last_event_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "last_event_time", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	},
	SessionTotalEvents: schema.Interface{
		ID:      "core.d8a.tech/sessions/total_events",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "total_events", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
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
