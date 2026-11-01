package transformations

import (
	"errors"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// ExtractionDependencies supplies the event column registries the worker uses, keyed by
// protocol ID, so early reads match stored values (page URL exclusions, geo lookups and
// so on). Without a registry only fields read straight from the request are available;
// page URL fields fail.
type ExtractionDependencies struct {
	Columns map[string]schema.ColumnsRegistry
}

var errExtraction = errors.New("transformations: extraction failed")

type eventView struct {
	input    Input
	protocol string
	values   map[string]any
	// event is built lazily from the hit to run column implementations; writes reset it.
	event      *schema.Event
	done       map[schema.InterfaceID]bool
	plan       *columnPlan
	planLoaded bool
	failed     bool
}

func newEventView(input Input) *eventView {
	return &eventView{input: input, protocol: input.Protocol, values: make(map[string]any)}
}

func (v *eventView) request() protocol.RequestParams { return loadCatalog().requests[v.protocol] }

func (v *eventView) parametersSupported() bool { return v.request().TextParamPrefix != "" }

func (v *eventView) pageLocationKey() string { return v.request().Fields["page_location"] }

// parameterPrefix returns the request prefix of a custom event parameter of the given type.
func (v *eventView) parameterPrefix(kind string) string {
	if kind == "number" {
		return v.request().NumberParamPrefix
	}
	return v.request().TextParamPrefix
}

// lookup backs the lazily patched __field calls of compiled predicates.
func (v *eventView) lookup(name string) (any, error) {
	var value any
	var err error
	switch name {
	case paramsNamespace, numericParamsNamespace:
		value = v.parameterMap(name == numericParamsNamespace)
	case urlParamsNamespace:
		value, err = v.urlParameterMap()
	default:
		value, err = v.field(name)
	}
	if err != nil {
		v.failed = true
		return nil, errExtraction
	}
	return value, nil
}

func (v *eventView) get(ref properties.TransformationFieldReference) (any, error) {
	switch {
	case ref.Parameter != "":
		return v.parameter(ref), nil
	case ref.URLParameter != "":
		query, err := v.pageQuery()
		if err != nil || query == nil || !query.Has(ref.URLParameter) {
			return nil, err
		}
		return query.Get(ref.URLParameter), nil
	}
	return v.field(ref.Field)
}

func (v *eventView) parameter(ref properties.TransformationFieldReference) any {
	if !v.parametersSupported() {
		return nil
	}
	values, exists := v.input.Hit.Request.QueryParams[v.parameterPrefix(ref.Type)+ref.Parameter]
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

func (v *eventView) parameterMap(numeric bool) map[string]any {
	kind := "string"
	if numeric {
		kind = "number"
	}
	prefix := v.parameterPrefix(kind)
	params := make(map[string]any)
	if !v.parametersSupported() {
		return params
	}
	for key := range v.input.Hit.Request.QueryParams {
		if name, ok := strings.CutPrefix(key, prefix); ok {
			params[name] = v.parameter(properties.TransformationFieldReference{Parameter: name, Type: kind})
		}
	}
	return params
}

// pageQuery parses the raw page URL as sent; utm_* columns read the same original URL.
func (v *eventView) pageQuery() (url.Values, error) {
	key := v.pageLocationKey()
	if key == "" {
		return nil, nil
	}
	raw := v.input.Hit.Request.QueryParams.Get(key)
	if raw == "" {
		return nil, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	return parsed.Query(), nil
}

func (v *eventView) urlParameterMap() (map[string]any, error) {
	query, err := v.pageQuery()
	result := make(map[string]any, len(query))
	for name, values := range query {
		if len(values) > 0 {
			result[name] = values[0]
		}
	}
	return result, err
}

func (v *eventView) field(name string) (any, error) {
	if value, ok := v.values[name]; ok {
		return value, nil
	}
	var value any
	var err error
	if name == IPAddressField {
		value = v.input.OriginalIP
		if value == "" {
			value = v.input.Hit.Request.IP
		}
	} else if plan := v.columnPlan(); plan != nil && plan.byField[name] != nil {
		value, err = v.columnValue(plan, name)
	} else {
		value, err = v.coreField(name)
	}
	if err != nil {
		return nil, err
	}
	value = normalizeValue(value)
	v.values[name] = value
	return value, nil
}

func (v *eventView) columnPlan() *columnPlan {
	if v.planLoaded {
		return v.plan
	}
	v.planLoaded = true
	registry := v.input.Dependencies.Columns[v.protocol]
	if registry == nil {
		return nil
	}
	v.plan = planFor(registry, v.input.Hit.PropertyID)
	return v.plan
}

func (v *eventView) columnValue(plan *columnPlan, name string) (any, error) {
	if v.event == nil {
		v.event = schema.NewEvent(v.input.Hit)
		v.done = make(map[schema.InterfaceID]bool)
	}
	for _, column := range plan.byField[name] {
		id := column.Implements().ID
		if v.done[id] {
			continue
		}
		if err := column.Write(v.event); err != nil {
			return nil, errExtraction
		}
		v.done[id] = true
	}
	return v.event.Values[name], nil
}

// coreField reads core fields directly from the request when no column registry is configured.
// Page URL fields need the protocols' page columns (URL exclusions, parsing), so they fail
// instead of being derived a second time here.
func (v *eventView) coreField(name string) (any, error) {
	hit := v.input.Hit
	switch name {
	case "name":
		return hit.EventName, nil
	case "user_id":
		if hit.UserID != nil {
			return *hit.UserID, nil
		}
		return nil, nil
	case "page_location", "page_hostname", "page_path":
		return nil, errExtraction
	}
	key := v.request().Fields[name]
	if key == "" {
		return nil, nil
	}
	return hit.Request.QueryParams.Get(key), nil
}

// normalizeValue maps column values onto the expression types: strings and float64
// numbers, with empty strings treated as missing.
func normalizeValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		if typed == "" {
			return nil
		}
		return typed
	case *string:
		if typed == nil || *typed == "" {
			return nil
		}
		return *typed
	}
	number := reflect.ValueOf(value)
	switch number.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(number.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(number.Uint())
	case reflect.Float32, reflect.Float64:
		return number.Float()
	case reflect.Pointer:
		if number.IsNil() {
			return nil
		}
		return normalizeValue(number.Elem().Interface())
	}
	return value
}

// invalidate drops memoized reads after a rule wrote, so later rules see the new state.
func (v *eventView) invalidate() {
	clear(v.values)
	v.event = nil
	v.done = nil
}

// write changes the hit. It does not drop memoized reads: callers invalidate once the
// rule's writes are done (see applyActions), since writes only read the raw request.
func (v *eventView) write(ref properties.TransformationFieldReference, value any, remove bool) error {
	params := v.input.Hit.Request.QueryParams
	if params == nil {
		params = make(url.Values)
		v.input.Hit.Request.QueryParams = params
	}
	text := formatValue(value)
	switch {
	case ref.Parameter != "":
		if !v.parametersSupported() {
			return nil
		}
		params.Del(v.parameterPrefix("string") + ref.Parameter)
		params.Del(v.parameterPrefix("number") + ref.Parameter)
		if !remove {
			params.Set(v.parameterPrefix(ref.Type)+ref.Parameter, text)
		}
		return nil
	case ref.URLParameter != "":
		return v.rewritePage(func(page *url.URL) bool { return setQueryParam(page, ref.URLParameter, text, remove) })
	}
	info := loadCatalog().fields[ref.Field]
	if info == nil {
		return nil
	}
	spec, ok := info.writes[v.protocol]
	if !ok {
		return nil
	}
	switch spec.kind {
	case writeQuery:
		if remove {
			params.Del(spec.key)
		} else {
			params.Set(spec.key, text)
		}
		v.syncHitField(ref.Field, text, remove)
	case writePageURLParam:
		return v.rewritePage(func(page *url.URL) bool { return setQueryParam(page, spec.key, text, remove) })
	case writePageURLElement:
		return v.rewritePage(func(page *url.URL) bool {
			if spec.key == "page_hostname" {
				if page.Host == text {
					return false
				}
				page.Host = text
				return true
			}
			if page.Path == text && page.RawPath == "" {
				return false
			}
			page.Path, page.RawPath = text, ""
			return true
		})
	case writeOverride:
		metadata := v.input.Hit.Metadata
		if metadata == nil {
			metadata = make(map[string]string)
			v.input.Hit.Metadata = metadata
		}
		delete(metadata, columns.OverrideMetadataKey(ref.Field))
		delete(metadata, columns.UnsetMetadataKey(ref.Field))
		if remove {
			metadata[columns.UnsetMetadataKey(ref.Field)] = "1"
		} else {
			metadata[columns.OverrideMetadataKey(ref.Field)] = text
		}
	}
	return nil
}

func (v *eventView) syncHitField(field, text string, remove bool) {
	switch field {
	case "name":
		v.input.Hit.EventName = text
	case "user_id":
		if remove {
			v.input.Hit.UserID = nil
		} else {
			v.input.Hit.UserID = &text
		}
	}
}

// rewritePage edits the raw page URL in the request. A missing URL is left alone, and so
// is the URL when mutate reports no change: re-serializing would re-encode and reorder its
// query string even though no value changed.
func (v *eventView) rewritePage(mutate func(*url.URL) bool) error {
	key := v.pageLocationKey()
	params := v.input.Hit.Request.QueryParams
	if key == "" || params.Get(key) == "" {
		return nil
	}
	page, err := url.Parse(params.Get(key))
	if err != nil {
		return errExtraction
	}
	if mutate(page) {
		params.Set(key, page.String())
	}
	return nil
}

// setQueryParam sets or removes one query parameter and reports whether the URL changed.
func setQueryParam(page *url.URL, name, value string, remove bool) bool {
	query := page.Query()
	current, present := query[name]
	if remove {
		if !present {
			return false
		}
		query.Del(name)
	} else {
		if len(current) == 1 && current[0] == value {
			return false
		}
		query.Set(name, value)
	}
	page.RawQuery = query.Encode()
	return true
}

// columnPlan lists, per field, the event columns to run (with dependencies) in worker order.
type columnPlan struct {
	byField map[string][]schema.EventColumn
	expires time.Time
}

type planKey struct {
	registry schema.ColumnsRegistry
	property string
}

const (
	planTTL      = 30 * time.Second
	maxPlanCount = 1024
)

var plans = struct {
	sync.Mutex
	entries map[planKey]*columnPlan
}{entries: make(map[planKey]*columnPlan)}

// planFor caches sorted column closures per registry and property; registries may resolve
// per-property columns, so entries expire.
func planFor(registry schema.ColumnsRegistry, property string) *columnPlan {
	cacheable := reflect.TypeOf(registry).Comparable()
	key := planKey{registry: registry, property: property}
	now := time.Now()
	if cacheable {
		plans.Lock()
		cached := plans.entries[key]
		plans.Unlock()
		if cached != nil && now.Before(cached.expires) {
			return cached
		}
	}
	cols, err := registry.Get(property)
	if err != nil {
		return nil
	}
	plan := buildPlan(cols.Event)
	plan.expires = now.Add(planTTL)
	if cacheable {
		plans.Lock()
		if len(plans.entries) >= maxPlanCount {
			clear(plans.entries)
		}
		plans.entries[key] = plan
		plans.Unlock()
	}
	return plan
}

func buildPlan(sorted []schema.EventColumn) *columnPlan {
	index := make(map[schema.InterfaceID]int, len(sorted))
	for i, column := range sorted {
		index[column.Implements().ID] = i
	}
	plan := &columnPlan{byField: make(map[string][]schema.EventColumn)}
	for i, column := range sorted {
		needed := map[int]bool{}
		var visit func(int)
		visit = func(at int) {
			if needed[at] {
				return
			}
			needed[at] = true
			for _, dependency := range sorted[at].DependsOn() {
				if position, ok := index[dependency.Interface]; ok {
					visit(position)
				}
			}
		}
		visit(i)
		closure := make([]schema.EventColumn, 0, len(needed))
		for position := range sorted {
			if needed[position] {
				closure = append(closure, sorted[position])
			}
		}
		plan.byField[column.Implements().Field.Name] = closure
	}
	return plan
}
