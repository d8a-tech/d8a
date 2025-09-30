package columns

import (
	"reflect" // nolint:depguard // it's not speed-sensitive

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// CoreInterfaces are the core columns that are always present in the schema.
var CoreInterfaces = struct {
	EventID               schema.Interface
	EventName             schema.Interface
	EventTimestamp        schema.Interface
	EventDate             schema.Interface
	EventDocumentTitle    schema.Interface
	EventDocumentLocation schema.Interface
	EventDocumentReferrer schema.Interface
	EventClientID         schema.Interface
	EventUserID           schema.Interface
	EventPropertyID       schema.Interface
	EventTrackingProtocol schema.Interface
	EventPlatform         schema.Interface
	EventIPAddress        schema.Interface
	EventPageLocation     schema.Interface
	EventGclid            schema.Interface
	EventDclid            schema.Interface
	EventSrsltid          schema.Interface
	EventAclid            schema.Interface
	EventAnid             schema.Interface
	SessionID             schema.Interface
	SessionDuration       schema.Interface
	SessionFirstEventTime schema.Interface
	SessionLastEventTime  schema.Interface
	SessionTotalEvents    schema.Interface
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
	EventDocumentTitle: schema.Interface{
		ID:      "core.d8a.tech/events/document_title",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "document_title", Type: arrow.BinaryTypes.String},
	},
	EventDocumentLocation: schema.Interface{
		ID:      "core.d8a.tech/events/document_location",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "document_location", Type: arrow.BinaryTypes.String},
	},
	EventDocumentReferrer: schema.Interface{
		ID:      "core.d8a.tech/events/document_referrer",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "document_referrer", Type: arrow.BinaryTypes.String},
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
	EventPageLocation: schema.Interface{
		ID:      "core.d8a.tech/events/page_location",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "page_location", Type: arrow.BinaryTypes.String},
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
