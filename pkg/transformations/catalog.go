// Package transformations compiles and evaluates early event transformations.
// Receiver execution and normalized persistence are separate integration concerns.
package transformations

import (
	"slices"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
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
	stringOperators = []string{
		"==", "!=", "in", "contains", "startsWith", "endsWith", "matches", "== nil", "!= nil",
	}
	numberOperators = []string{"==", "!=", "in", "<", "<=", ">", ">=", "== nil", "!= nil"}
	excludedFields  = map[string]bool{
		"id": true, "client_id": true, "property_id": true, "property_name": true,
		"ip_address": true, "geo_sub_continent": true, "geo_metro": true,
		// Device detection (UA parsing) is too expensive to run on every incoming hit.
		"device_category": true, "device_mobile_brand_name": true, "device_mobile_model_name": true,
		"device_operating_system": true, "device_operating_system_version": true,
		"device_web_browser": true, "device_web_browser_version": true,
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

// Catalog lists the fields early transformations can read and write with a set of
// protocols, and compiles rules against them.
type Catalog struct {
	fields     map[string]*fieldInfo
	list       []FieldCapability
	parameters []ParameterHint
	// requests holds each protocol's request layout by protocol ID; aliases maps the
	// request parameters of core fields (wire names such as "dl") to field names.
	requests map[string]protocol.RequestParams
	aliases  map[string]string
}

// Fields returns the fields transformations can read and write. It is derived from the
// protocols' event columns; device fields are deliberately excluded. Custom parameters
// ("parameter") and page URL parameters ("url_parameter") are generic reference kinds
// listed after the named fields.
func (c *Catalog) Fields() []FieldCapability {
	result := make([]FieldCapability, len(c.list))
	for i, field := range c.list {
		field.ReadProtocols = append([]string(nil), field.ReadProtocols...)
		field.WriteProtocols = append([]string(nil), field.WriteProtocols...)
		field.Operators = append([]string(nil), field.Operators...)
		result[i] = field
	}
	return result
}

// KnownParameters returns custom event parameters that the protocols already map to columns.
func (c *Catalog) KnownParameters() []ParameterHint {
	return append([]ParameterHint(nil), c.parameters...)
}

// NewCatalog builds the catalog for the given protocols, in the order they are listed.
func NewCatalog(protocols ...protocol.Protocol) *Catalog {
	data := &Catalog{
		fields:   make(map[string]*fieldInfo),
		requests: make(map[string]protocol.RequestParams),
		aliases:  make(map[string]string),
	}
	settings := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{}))
	params := make(map[string]string)
	var ids, paramIDs []string
	for _, implementation := range protocols {
		protocolID := implementation.ID()
		ids = append(ids, protocolID)
		request := implementation.RequestParams()
		data.requests[protocolID] = request
		if request.TextParamPrefix != "" {
			paramIDs = append(paramIDs, protocolID)
		}
		for field, key := range request.Fields {
			data.aliases[key] = field
		}
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
			if name, ok := customParameter(request, queryParam); ok {
				params[name] = kind
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
			if spec, ok := writeSpecFor(request, name, queryParam, pageURLParam); ok {
				info.writes[protocolID] = spec
				info.capability.WriteProtocols = appendUnique(info.capability.WriteProtocols, protocolID)
			}
		}
	}
	data.fields[IPAddressField] = &fieldInfo{capability: FieldCapability{
		Field: IPAddressField, Type: "string", Category: CategoryRequest,
		ReadProtocols: append([]string(nil), ids...), ConditionOnly: true,
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
		sortProtocols(info.capability.ReadProtocols, ids)
		sortProtocols(info.capability.WriteProtocols, ids)
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
			ReadProtocols: paramIDs, WriteProtocols: paramIDs, Removable: true, Operators: operators,
		})
	}
	data.list = append(data.list, FieldCapability{
		Field: "url_parameter", Type: "string", Category: CategoryPage,
		ReadProtocols: ids, WriteProtocols: ids, Removable: true, Operators: stringOperators,
	})
	for name, kind := range params {
		data.parameters = append(data.parameters, ParameterHint{Name: name, Type: kind})
	}
	sort.Slice(data.parameters, func(i, j int) bool { return data.parameters[i].Name < data.parameters[j].Name })
	return data
}

// customParameter returns the custom event parameter a request parameter holds, if any.
func customParameter(request protocol.RequestParams, queryParam string) (string, bool) {
	for _, prefix := range []string{request.TextParamPrefix, request.NumberParamPrefix} {
		if name, ok := strings.CutPrefix(queryParam, prefix); prefix != "" && ok {
			return name, true
		}
	}
	return "", false
}

func writeSpecFor(request protocol.RequestParams, name, queryParam, pageURLParam string) (writeSpec, bool) {
	if key := request.Fields[name]; key != "" {
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
	case queryParam != "" && !slices.Contains(request.ReadOnly, queryParam):
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

func (c *Catalog) canonicalField(name string) string {
	if alias, ok := c.aliases[name]; ok {
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

// sortProtocols orders protocol IDs as the catalog's protocols are listed.
func sortProtocols(list, order []string) {
	sort.Slice(list, func(i, j int) bool { return slices.Index(order, list[i]) < slices.Index(order, list[j]) })
}
