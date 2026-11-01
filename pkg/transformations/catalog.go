// Package transformations compiles and evaluates early event transformations.
// Receiver execution and normalized persistence are separate integration concerns.
package transformations

import (
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protocol/matomo"
)

const (
	MaxRules            = 100
	MaxActions          = 20
	MaxNameLength       = 100
	MaxValueLength      = 8192
	MaxExpressionLength = 16384
	MaxPatternLength    = 2000
	// MaxReplaceInputLength bounds the text a replace action scans; longer values are left as is.
	MaxReplaceInputLength = 16384
)

// DefaultReplacement is what replace actions insert when no replacement is configured.
const DefaultReplacement = "[REDACTED]"

// Field categories group the catalog for editors.
const (
	CategoryEvent         = "event"
	CategoryPage          = "page"
	CategoryTrafficSource = "traffic_source"
	CategoryGeo           = "geo"
	CategoryDevice        = "device"
	CategoryEcommerce     = "ecommerce"
	CategoryRequest       = "request"
	CategoryOther         = "other"
)

// IPAddressField is the condition-only, unmasked client IP of the incoming request.
const IPAddressField = "ip_address"

// FieldCapability describes the early field contract, not the warehouse schema.
type FieldCapability struct {
	Field          string   `json:"field"`
	Type           string   `json:"type"`
	Category       string   `json:"category"`
	ReadProtocols  []string `json:"read_protocols"`
	WriteProtocols []string `json:"write_protocols"`
	Removable      bool     `json:"removable"`
	ConditionOnly  bool     `json:"condition_only"`
	Operators      []string `json:"operators"`
}

// ParameterHint names a custom event parameter the tracker already knows.
type ParameterHint struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

var (
	allProtocols    = []string{"ga4", "d8a", "matomo"}
	paramProtocols  = []string{"ga4", "d8a"}
	stringOperators = []string{
		"==", "!=", "in", "contains", "startsWith", "endsWith", "matches", "== nil", "!= nil",
	}
	numberOperators = []string{"==", "!=", "in", "<", "<=", ">", ">=", "== nil", "!= nil"}
	aliases         = map[string]string{
		"en": "name", "dl": "page_location", "url": "page_location",
		"dt": "page_title", "action_name": "page_title", "dr": "page_referrer",
		"urlref": "page_referrer", "uid": "user_id",
	}
	// Request keys that identify the hit or drive protocol plumbing stay read-only.
	readOnlyQueryParams = map[string]bool{
		"tid": true, "cid": true, "sid": true, "sct": true, "_et": true, "_s": true, "_p": true,
		"v": true, "idsite": true, "_id": true, "rec": true, "_idn": true, "_idvc": true,
		"ep.ga_session_id": true, "epn.ga_session_number": true,
	}
	excludedFields = map[string]bool{
		"id": true, "client_id": true, "property_id": true, "property_name": true,
		"ip_address": true, "geo_sub_continent": true, "geo_metro": true,
		// Device detection (UA parsing) is too expensive to run on every incoming hit.
		"device_category": true, "device_mobile_brand_name": true, "device_mobile_model_name": true,
		"device_operating_system": true, "device_operating_system_version": true,
		"device_web_browser": true, "device_web_browser_version": true,
	}
	coreWireKeys = map[string]map[string]string{
		"ga4":    {"name": "en", "page_location": "dl", "page_title": "dt", "page_referrer": "dr", "user_id": "uid"},
		"d8a":    {"name": "en", "page_location": "dl", "page_title": "dt", "page_referrer": "dr", "user_id": "uid"},
		"matomo": {"page_location": "url", "page_title": "action_name", "page_referrer": "urlref", "user_id": "uid"},
	}
	overrideFields = map[string]bool{"geo_continent": true, "geo_country": true, "geo_region": true, "geo_city": true}
)

type writeKind int

const (
	writeQuery writeKind = iota + 1
	writePageURLParam
	writePageURLElement
	writeOverride
)

type writeSpec struct {
	kind writeKind
	key  string
}

type fieldInfo struct {
	capability FieldCapability
	writes     map[string]writeSpec
}

// pageDependent reports whether writing the field rewrites the page location.
func (f *fieldInfo) pageDependent() bool {
	for _, spec := range f.writes {
		if spec.kind == writePageURLParam || spec.kind == writePageURLElement {
			return true
		}
	}
	return false
}

type catalogData struct {
	fields     map[string]*fieldInfo
	list       []FieldCapability
	parameters []ParameterHint
}

var (
	catalogOnce sync.Once
	catalogVal  *catalogData
)

func loadCatalog() *catalogData {
	catalogOnce.Do(func() { catalogVal = buildCatalog() })
	return catalogVal
}

// Catalog returns the fields early transformations can read and write. It is derived from
// the event columns of the built-in protocols; device fields are deliberately excluded.
// Custom parameters ("parameter") and page URL parameters ("url_parameter") are generic
// reference kinds listed after the named fields.
func Catalog() []FieldCapability {
	source := loadCatalog().list
	result := make([]FieldCapability, len(source))
	for i, field := range source {
		field.ReadProtocols = append([]string(nil), field.ReadProtocols...)
		field.WriteProtocols = append([]string(nil), field.WriteProtocols...)
		field.Operators = append([]string(nil), field.Operators...)
		result[i] = field
	}
	return result
}

// KnownParameters returns custom event parameters that built-in protocols already map to columns.
func KnownParameters() []ParameterHint {
	return append([]ParameterHint(nil), loadCatalog().parameters...)
}

func catalogProtocols() []protocol.Protocol {
	registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{}))
	converter := currency.NewDummyConverter(1)
	return []protocol.Protocol{
		ga4.NewGA4Protocol(converter, registry),
		d8a.NewD8AProtocol(converter, registry),
		matomo.NewMatomoProtocol(matomo.NewFromIDSiteExtractor(registry), registry),
	}
}

func buildCatalog() *catalogData {
	data := &catalogData{fields: make(map[string]*fieldInfo)}
	settings := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{}))
	params := make(map[string]string)
	for _, implementation := range catalogProtocols() {
		protocolID := implementation.ID()
		cols, err := columnset.DefaultColumnRegistry(implementation, settings).Get("")
		if err != nil {
			panic("transformations: building field catalog: " + err.Error())
		}
		for _, column := range cols.Event {
			field := column.Implements().Field
			kind := valueType(field.Type)
			name := field.Name
			if kind == "" || excludedFields[name] {
				continue
			}
			queryParam, pageURLParam := columns.EventColumnSources(column)
			if strings.HasPrefix(queryParam, "ep.") || strings.HasPrefix(queryParam, "epn.") {
				params[strings.SplitN(queryParam, ".", 2)[1]] = kind
				continue
			}
			info := data.fields[name]
			if info == nil {
				info = &fieldInfo{
					capability: FieldCapability{Field: name, Type: kind, Category: category(name)},
					writes:     make(map[string]writeSpec),
				}
				data.fields[name] = info
			}
			info.capability.ReadProtocols = appendUnique(info.capability.ReadProtocols, protocolID)
			if spec, ok := writeSpecFor(protocolID, name, queryParam, pageURLParam); ok {
				info.writes[protocolID] = spec
				info.capability.WriteProtocols = appendUnique(info.capability.WriteProtocols, protocolID)
			}
		}
	}
	data.fields[IPAddressField] = &fieldInfo{capability: FieldCapability{
		Field: IPAddressField, Type: "string", Category: CategoryRequest,
		ReadProtocols: append([]string(nil), allProtocols...), ConditionOnly: true,
	}, writes: map[string]writeSpec{}}
	names := make([]string, 0, len(data.fields))
	for name, info := range data.fields {
		names = append(names, name)
		elementOnly := name == "page_hostname" || name == "page_path"
		info.capability.Removable = len(info.capability.WriteProtocols) > 0 && name != "name" && !elementOnly
		info.capability.Operators = stringOperators
		if info.capability.Type == "number" {
			info.capability.Operators = numberOperators
		}
		if name == IPAddressField {
			info.capability.Operators = append(append([]string(nil), stringOperators...), "inCidr")
		}
		sortProtocols(info.capability.ReadProtocols)
		sortProtocols(info.capability.WriteProtocols)
	}
	sort.Strings(names)
	for _, name := range names {
		data.list = append(data.list, data.fields[name].capability)
	}
	for _, kind := range []string{"string", "number"} {
		operators := stringOperators
		if kind == "number" {
			operators = numberOperators
		}
		data.list = append(data.list, FieldCapability{
			Field: "parameter", Type: kind, Category: CategoryEvent,
			ReadProtocols: paramProtocols, WriteProtocols: paramProtocols, Removable: true, Operators: operators,
		})
	}
	data.list = append(data.list, FieldCapability{
		Field: "url_parameter", Type: "string", Category: CategoryPage,
		ReadProtocols: allProtocols, WriteProtocols: allProtocols, Removable: true, Operators: stringOperators,
	})
	for name, kind := range params {
		data.parameters = append(data.parameters, ParameterHint{Name: name, Type: kind})
	}
	sort.Slice(data.parameters, func(i, j int) bool { return data.parameters[i].Name < data.parameters[j].Name })
	return data
}

func writeSpecFor(protocolID, name, queryParam, pageURLParam string) (writeSpec, bool) {
	if key := coreWireKeys[protocolID][name]; key != "" {
		return writeSpec{kind: writeQuery, key: key}, true
	}
	if name == "name" {
		return writeSpec{}, false
	}
	switch {
	case name == "page_hostname" || name == "page_path":
		return writeSpec{kind: writePageURLElement, key: name}, true
	case overrideFields[name]:
		return writeSpec{kind: writeOverride, key: name}, true
	case pageURLParam != "":
		return writeSpec{kind: writePageURLParam, key: pageURLParam}, true
	case queryParam != "" && !readOnlyQueryParams[queryParam]:
		return writeSpec{kind: writeQuery, key: queryParam}, true
	}
	return writeSpec{}, false
}

func valueType(t arrow.DataType) string {
	switch {
	case t == nil:
		return ""
	case t.ID() == arrow.STRING || t.ID() == arrow.LARGE_STRING:
		return "string"
	case arrow.IsInteger(t.ID()) || arrow.IsFloating(t.ID()):
		return "number"
	}
	return ""
}

func category(name string) string {
	switch {
	case name == "name" || name == "user_id":
		return CategoryEvent
	case strings.HasPrefix(name, "page_"):
		return CategoryPage
	case strings.HasPrefix(name, "utm_") || strings.HasPrefix(name, "click_id_"):
		return CategoryTrafficSource
	case strings.HasPrefix(name, "geo_"):
		return CategoryGeo
	case strings.HasPrefix(name, "device_"):
		return CategoryDevice
	case strings.HasPrefix(name, "ecommerce_") || strings.HasPrefix(name, "transaction_"):
		return CategoryEcommerce
	}
	return CategoryOther
}

func canonicalField(name string) string {
	if alias, ok := aliases[name]; ok {
		return alias
	}
	return name
}

func appendUnique(list []string, value string) []string {
	for _, item := range list {
		if item == value {
			return list
		}
	}
	return append(list, value)
}

func sortProtocols(list []string) {
	order := map[string]int{"ga4": 0, "d8a": 1, "matomo": 2}
	sort.Slice(list, func(i, j int) bool { return order[list[i]] < order[list[j]] })
}
