// Package transformations compiles and evaluates early event transformations.
// Receiver execution and normalized persistence are separate integration concerns.
package transformations

const (
	MaxRules            = 100
	MaxActions          = 20
	MaxNameLength       = 100
	MaxValueLength      = 8192
	MaxExpressionLength = 16384
	MaxCachedPrograms   = 256
)

// FieldCapability describes the early field contract, not the warehouse schema.
type FieldCapability struct {
	Field          string   `json:"field"`
	Type           string   `json:"type"`
	ReadProtocols  []string `json:"read_protocols"`
	WriteProtocols []string `json:"write_protocols"`
	Removable      bool     `json:"removable"`
	Operators      []string `json:"operators"`
}

var fieldNames = []string{
	"name", "page_location", "page_title", "page_hostname", "page_path", "page_referrer",
	"device_web_browser", "device_operating_system", "device_category",
	"geo_country", "geo_region", "geo_city", "user_id",
}

var aliases = map[string]string{
	"en": "name", "dl": "page_location", "url": "page_location",
	"dt": "page_title", "action_name": "page_title", "dr": "page_referrer",
	"urlref": "page_referrer", "uid": "user_id",
}

// Catalog returns owned capability data. Custom parameters are GA4/Native only.
func Catalog() []FieldCapability {
	result := make([]FieldCapability, 0, len(fieldNames)+2)
	for _, name := range fieldNames {
		field := FieldCapability{
			Field: name, Type: "string", ReadProtocols: []string{"ga4", "d8a", "matomo"},
			Operators: []string{"==", "!=", "in", "contains", "startsWith", "endsWith", "matches", "== nil", "!= nil"},
		}
		switch name {
		case "name":
			field.WriteProtocols = []string{"ga4", "d8a"}
		case "page_location", "page_title", "page_referrer", "user_id":
			field.WriteProtocols = []string{"ga4", "d8a", "matomo"}
			field.Removable = true
		}
		result = append(result, field)
	}
	for _, kind := range []string{"string", "number"} {
		operators := []string{"==", "!=", "in", "<", "<=", ">", ">=", "== nil", "!= nil"}
		if kind == "string" {
			operators = append(operators, "contains", "startsWith", "endsWith", "matches")
		}
		result = append(result, FieldCapability{
			Field: "parameter", Type: kind, ReadProtocols: []string{"ga4", "d8a"},
			WriteProtocols: []string{"ga4", "d8a"}, Removable: true, Operators: operators,
		})
	}
	return result
}

func canonicalField(name string) string {
	if alias, ok := aliases[name]; ok {
		return alias
	}
	return name
}

func writable(name string) bool {
	switch name {
	case "name", "page_location", "page_title", "page_referrer", "user_id":
		return true
	default:
		return false
	}
}
