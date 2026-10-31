package transformations

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
)

// ExtractionDependencies supplies masked-IP enrichment. Use the existing geo
// column factory as Geo to share its bounded lookup cache; nil means unavailable.
type ExtractionDependencies struct{ Geo dbip.LookupProvider }

type eventView struct {
	protocol     string
	hit          *hits.Hit
	settings     *properties.Settings
	deps         ExtractionDependencies
	values       map[string]any
	deviceLoaded bool
	geoLoaded    bool
}

func newEventView(id string, hit *hits.Hit, settings *properties.Settings, deps ExtractionDependencies) *eventView {
	return &eventView{protocol: id, hit: hit, settings: settings, deps: deps, values: make(map[string]any)}
}

func (v *eventView) supported() bool {
	return v.protocol == "ga4" || v.protocol == "d8a" || v.protocol == "matomo"
}
func (v *eventView) parametersSupported() bool { return v.protocol == "ga4" || v.protocol == "d8a" }

func (v *eventView) wireKey(ref properties.TransformationFieldReference) string {
	if ref.Parameter != "" {
		if !v.parametersSupported() {
			return ""
		}
		if ref.Type == "number" {
			return "epn." + ref.Parameter
		}
		return "ep." + ref.Parameter
	}
	if !v.supported() {
		return ""
	}
	if v.protocol == "matomo" {
		return map[string]string{
			"page_location": "url", "page_title": "action_name", "page_referrer": "urlref", "user_id": "uid",
		}[ref.Field]
	}
	return map[string]string{
		"name": "en", "page_location": "dl", "page_title": "dt", "page_referrer": "dr", "user_id": "uid",
	}[ref.Field]
}

func (v *eventView) get(ref properties.TransformationFieldReference) (any, error) {
	if ref.Parameter != "" {
		return v.parameter(ref), nil
	}
	if value, ok := v.values[ref.Field]; ok {
		return value, nil
	}
	if !v.supported() {
		return v.values[ref.Field], nil
	}
	value, err := v.readField(ref.Field)
	if err != nil {
		return nil, err
	}
	v.values[ref.Field] = value
	return value, nil
}

func (v *eventView) parameter(ref properties.TransformationFieldReference) any {
	key := v.wireKey(ref)
	if key == "" {
		return nil
	}
	values, exists := v.hit.Request.QueryParams[key]
	if !exists || len(values) == 0 {
		return nil
	}
	if ref.Type != "number" {
		return values[0]
	}
	number, err := strconv.ParseFloat(values[0], 64)
	if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
		return nil
	}
	return number
}

func (v *eventView) readField(field string) (any, error) {
	switch field {
	case "name":
		return v.hit.EventName, nil
	case "user_id":
		if v.hit.UserID != nil {
			return *v.hit.UserID, nil
		}
		return v.values[field], nil
	case "page_hostname", "page_path":
		location, err := v.get(properties.TransformationFieldReference{Field: "page_location"})
		if err != nil || location == nil {
			return location, err
		}
		text, ok := location.(string)
		if !ok {
			return nil, fmt.Errorf("invalid page location type")
		}
		parsed, err := url.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("invalid page location")
		}
		if field == "page_hostname" {
			return parsed.Hostname(), nil
		}
		return parsed.Path, nil
	case "device_web_browser", "device_operating_system", "device_category":
		v.loadDevice()
		return v.values[field], nil
	case "geo_country", "geo_region", "geo_city":
		if err := v.loadGeo(); err != nil {
			return nil, err
		}
		return v.values[field], nil
	default:
		ref := properties.TransformationFieldReference{Field: field}
		key := v.wireKey(ref)
		values, ok := v.hit.Request.QueryParams[key]
		if !ok || len(values) == 0 {
			return v.values[field], nil
		}
		if field != "page_location" || values[0] == "" {
			return values[0], nil
		}
		var excluded []string
		if v.settings != nil {
			excluded = v.settings.ExcludedURLParamsSafe()
		}
		cleaned, _, err := columns.StripExcludedParams(values[0], excluded)
		if err != nil {
			return nil, fmt.Errorf("invalid page location")
		}
		return cleaned, nil
	}
}

func (v *eventView) loadDevice() {
	if v.deviceLoaded {
		return
	}
	v.deviceLoaded = true
	info := columns.DeviceInfoFromHeaders(v.hit.Request.Headers)
	if info == nil {
		return
	}
	if info.Client != nil && info.Client.Name != "" {
		v.values["device_web_browser"] = info.Client.Name
	}
	if info.OS != nil && info.OS.Name != "" {
		v.values["device_operating_system"] = info.OS.Name
	}
	if info.Device != nil && info.Device.Type != "" {
		v.values["device_category"] = info.Device.Type
	}
}

func (v *eventView) loadGeo() error {
	if v.geoLoaded {
		return nil
	}
	v.geoLoaded = true
	if v.deps.Geo == nil {
		return nil
	}
	ip, err := netip.ParseAddr(v.hit.Request.IP)
	if err != nil {
		return nil
	} // Downstream columns also treat an invalid IP as absent.
	record, err := v.deps.Geo.Lookup(ip)
	if errors.Is(err, dbip.ErrUnavailable) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("geolocation extraction failed")
	}
	if record == nil {
		return nil
	}
	v.values["geo_country"] = record.Country
	v.values["geo_city"] = record.City
	if record.Region != "" {
		v.values["geo_region"] = record.Region
	}
	return nil
}

func (v *eventView) environment(fields []string) (map[string]any, error) {
	env := make(map[string]any, len(fields))
	for _, field := range fields {
		if field == "params" || field == "numeric_params" {
			kind, prefix := "string", "ep."
			if field == "numeric_params" {
				kind, prefix = "number", "epn."
			}
			params := make(map[string]any)
			if v.parametersSupported() {
				for key := range v.hit.Request.QueryParams {
					if strings.HasPrefix(key, prefix) {
						name := strings.TrimPrefix(key, prefix)
						params[name] = v.parameter(properties.TransformationFieldReference{Parameter: name, Type: kind})
					}
				}
			}
			env[field] = params
			continue
		}
		value, err := v.get(properties.TransformationFieldReference{Field: field})
		if err != nil {
			return nil, err
		}
		env[field] = value
	}
	return env, nil
}

func (v *eventView) write(ref properties.TransformationFieldReference, value any, remove bool) {
	key := v.wireKey(ref)
	if key == "" {
		return
	}
	delete(v.values, ref.Field)
	if ref.Field == "page_location" {
		delete(v.values, "page_hostname")
		delete(v.values, "page_path")
	}
	params := v.hit.Request.QueryParams
	if params == nil {
		params = make(url.Values)
		v.hit.Request.QueryParams = params
	}
	if ref.Parameter != "" && !remove {
		params.Del("ep." + ref.Parameter)
		params.Del("epn." + ref.Parameter)
	}
	if remove {
		params.Del(key)
		if ref.Field == "user_id" {
			v.hit.UserID = nil
		}
		return
	}
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case float64:
		text = strconv.FormatFloat(typed, 'g', -1, 64)
	}
	params.Set(key, text)
	if ref.Field == "name" {
		v.hit.EventName = text
	}
	if ref.Field == "user_id" {
		v.hit.UserID = &text
	}
}
