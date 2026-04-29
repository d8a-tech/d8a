package matomo

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
)

type matomoProtocol struct {
	extractor              protocol.PropertyIDExtractor
	extraTrackingEndpoints []string
}

func (p *matomoProtocol) ID() string {
	return "matomo"
}

func (p *matomoProtocol) Hits(fhCtx *fasthttp.RequestCtx, request *hits.ParsedRequest) ([]*hits.Hit, error) {
	body := bytes.TrimSpace(request.Body)
	if len(body) > 0 && body[0] == '{' {
		var payload struct {
			Requests []string `json:"requests"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}

		theHits := make([]*hits.Hit, 0, len(payload.Requests))
		for _, requestLine := range payload.Requests {
			requestLine = strings.TrimSpace(requestLine)
			requestLine = strings.TrimPrefix(requestLine, "?")
			params, err := url.ParseQuery(requestLine)
			if err != nil {
				return nil, err
			}
			hit, err := createHitFromParams(p, params, request, fhCtx)
			if err != nil {
				return nil, err
			}
			theHits = append(theHits, hit)
		}
		return theHits, nil
	}

	hit, err := createHitFromParams(p, request.QueryParams, request, fhCtx)
	if err != nil {
		return nil, err
	}
	return []*hits.Hit{hit}, nil
}

func (p *matomoProtocol) Endpoints() []protocol.ProtocolEndpoint {
	paths := make([]string, 0, 1+len(p.extraTrackingEndpoints))
	paths = append(paths, "/matomo.php")
	paths = append(paths, p.extraTrackingEndpoints...)

	endpoints := make([]protocol.ProtocolEndpoint, 0, len(paths))
	for _, trackingPath := range uniqueTrackingEndpoints(paths) {
		endpoints = append(endpoints, protocol.ProtocolEndpoint{
			Methods: []string{fasthttp.MethodPost, fasthttp.MethodGet},
			Path:    trackingPath,
		})
	}

	return endpoints
}

func (p *matomoProtocol) Interfaces() any {
	return ProtocolInterfaces
}

func (p *matomoProtocol) Columns() schema.Columns {
	return schema.Columns{
		Event:              eventColumns,
		Session:            sessionColumns,
		SessionScopedEvent: sseColumns,
	}
}

type MatomoProtocolOption func(*matomoProtocol)

func WithExtraTrackingEndpoints(paths []string) MatomoProtocolOption {
	return func(p *matomoProtocol) {
		p.extraTrackingEndpoints = append([]string(nil), paths...)
	}
}

func NewMatomoProtocol(extractor protocol.PropertyIDExtractor, opts ...MatomoProtocolOption) protocol.Protocol {
	p := &matomoProtocol{extractor: extractor}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func uniqueTrackingEndpoints(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	unique := make([]string, 0, len(paths))
	for _, rawPath := range paths {
		path := normalizeTrackingEndpoint(rawPath)
		if path == "" {
			continue
		}
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		unique = append(unique, path)
	}
	return unique
}

func normalizeTrackingEndpoint(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

type fromIDSiteExtractor struct {
	psr properties.SettingsRegistry
}

func (e *fromIDSiteExtractor) PropertyID(ctx *protocol.RequestContext) (string, error) {
	idSite := ctx.Parsed.QueryParams.Get("idsite")
	if idSite == "" {
		return "", errors.New("missing idsite")
	}
	property, err := e.psr.GetByMeasurementID(idSite)
	if err != nil {
		return "", err
	}
	return property.PropertyID, nil
}

// NewFromIDSiteExtractor creates a PropertyIDExtractor that extracts
// property ID from the "idsite" query parameter using the property settings registry.
func NewFromIDSiteExtractor(psr properties.SettingsRegistry) protocol.PropertyIDExtractor {
	return &fromIDSiteExtractor{psr: psr}
}

func createHitFromParams(
	p *matomoProtocol,
	params url.Values,
	req *hits.ParsedRequest,
	fhCtx *fasthttp.RequestCtx,
) (*hits.Hit, error) {
	queryParams := url.Values{}
	for key, values := range params {
		for _, value := range values {
			queryParams.Add(key, value)
		}
	}

	requestCopy := req.Clone()
	requestCopy.QueryParams = queryParams

	ctx := &protocol.RequestContext{
		Parsed:   requestCopy,
		FastHttp: fhCtx,
	}

	propertyID, err := p.extractor.PropertyID(ctx)
	if err != nil {
		return nil, err
	}

	hit := hits.New()
	hit.ClientID = clientIDFromParams(queryParams)
	hit.AuthoritativeClientID = hit.ClientID
	hit.PropertyID = propertyID
	hit.EventName = deriveEventName(queryParams)

	userID := queryParams.Get("uid")
	if userID != "" {
		hit.UserID = &userID
	}

	hit.Request = requestCopy

	return hit, nil
}

func deriveEventName(params url.Values) string {
	if params.Get("idgoal") == "0" && params.Get("ec_id") != "" {
		return ecOrderEventType
	}
	if params.Get("idgoal") != "" {
		return goalConversionEventType
	}
	if params.Get("ma_id") != "" && params.Get("ma_mt") == "video" {
		return videoPlayEventType
	}
	if params.Get("download") != "" {
		return downloadEventType
	}
	if params.Get("link") != "" {
		return outlinkEventType
	}
	if _, hasSearch := params["search"]; hasSearch {
		return siteSearchEventType
	}
	if params.Get("c_i") != "" {
		return contentInteractionType
	}
	if params.Get("c_n") != "" {
		return contentImpressionType
	}
	if params.Get("e_c") != "" && params.Get("e_a") != "" {
		return customEventType
	}
	return protocol.PageViewEventType
}

func clientIDFromParams(params url.Values) hits.ClientID {
	if clientID := params.Get("_id"); clientID != "" {
		return hits.ClientID(clientID)
	}
	if clientID := params.Get("cid"); clientID != "" {
		return hits.ClientID(clientID)
	}
	return hits.ClientID(uuid.New().String())
}
