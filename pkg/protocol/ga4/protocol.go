package ga4

import (
	_ "embed"
	"errors"
	"net/url"
	"strings"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/valyala/fasthttp"
)

type ga4Protocol struct {
	converter currency.Converter

	propertyIDExtractor protocol.PropertyIDExtractor
}

func (p *ga4Protocol) ID() string {
	return "ga4"
}

func (p *ga4Protocol) Hits(reqCtx *fasthttp.RequestCtx, request *hits.ParsedRequest) ([]*hits.Hit, error) {
	reqCtx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	reqCtx.Response.Header.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	reqCtx.Response.Header.Set("Access-Control-Allow-Headers", "Content-Type, X-Requested-With")
	reqCtx.Response.Header.Set("Access-Control-Max-Age", "86400")

	// Parse body into lines (each line represents a hit)
	bodyStr := strings.TrimSpace(string(request.Body))
	var bodyLines []string
	if bodyStr != "" {
		bodyLines = strings.Split(bodyStr, "\n")
	}

	ctx := &protocol.RequestContext{
		FastHttp: reqCtx,
		Parsed:   request,
	}

	// If no body lines, create one hit with just query parameters
	if len(bodyLines) == 0 {
		hit, err := p.createHitFromQueryParams(ctx, request.Body)
		if err != nil {
			return nil, err
		}
		return []*hits.Hit{hit}, nil
	}

	// Pre-allocate slice with expected capacity
	theHits := make([]*hits.Hit, 0, len(bodyLines))

	// Create a hit for each line in the body
	for _, line := range bodyLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue // Skip empty lines
		}

		hit, err := p.createHitFromLine(ctx, line, request.Body)
		if err != nil {
			return nil, err
		}
		theHits = append(theHits, hit)
	}

	return theHits, nil
}

// createHitBase creates a hit with common fields populated from the request
func (p *ga4Protocol) createHitBase(ctx *protocol.RequestContext, _ []byte) (*hits.Hit, error) {
	hit := hits.New()

	clientID, err := p.ClientID(ctx.Parsed)
	if err != nil {
		return nil, err
	}
	hit.ClientID = hits.ClientID(clientID)
	hit.AuthoritativeClientID = hit.ClientID

	hit.PropertyID, err = p.PropertyID(ctx)
	if err != nil {
		return nil, err
	}

	hit.UserID, err = p.UserID(ctx.Parsed)
	if err != nil {
		return nil, err
	}

	hit.EventName, err = p.EventName(ctx.Parsed)
	if err != nil {
		return nil, err
	}

	hit.Request = ctx.Parsed.Clone()

	return hit, nil
}

// createHitFromQueryParams creates a hit using only query parameters
func (p *ga4Protocol) createHitFromQueryParams(ctx *protocol.RequestContext, body []byte) (*hits.Hit, error) {
	hit, err := p.createHitBase(ctx, body)
	if err != nil {
		return nil, err
	}

	queryParams := url.Values{}
	for key, values := range ctx.Parsed.QueryParams {
		for _, value := range values {
			queryParams.Add(key, value)
		}
	}
	hit.Request.QueryParams = queryParams

	return hit, nil
}

// createHitFromLine creates a hit by merging query parameters with line-specific parameters
func (p *ga4Protocol) createHitFromLine(ctx *protocol.RequestContext, line string, body []byte) (*hits.Hit, error) {
	mergedParams, err := p.mergeQueryParamsWithLine(ctx.Parsed.QueryParams, line)
	if err != nil {
		return nil, err
	}

	// Create a temporary request with merged parameters for extracting common fields
	requestCopy := ctx.Parsed.Clone()
	requestCopy.QueryParams = mergedParams

	ctxCopy := &protocol.RequestContext{
		FastHttp: ctx.FastHttp,
		Parsed:   requestCopy,
	}

	return p.createHitFromMergedParams(ctxCopy, ctx.Parsed.Body, mergedParams)
}

// mergeQueryParamsWithLine merges query parameters with line parameters (line params override)
func (p *ga4Protocol) mergeQueryParamsWithLine(queryParams url.Values, line string) (url.Values, error) {
	// Parse line parameters
	lineParams, err := url.ParseQuery(line)
	if err != nil {
		return nil, err
	}

	// Start with query parameters as base
	mergedParams := url.Values{}
	for key, values := range queryParams {
		for _, value := range values {
			mergedParams.Add(key, value)
		}
	}

	// Override with line parameters
	for key, values := range lineParams {
		mergedParams.Del(key) // Remove existing values
		for _, value := range values {
			mergedParams.Add(key, value)
		}
	}

	return mergedParams, nil
}

// createHitFromMergedParams creates a hit using merged parameters
func (p *ga4Protocol) createHitFromMergedParams(
	ctx *protocol.RequestContext,
	body []byte,
	mergedParams url.Values,
) (*hits.Hit, error) {
	hit, err := p.createHitBase(ctx, body)
	if err != nil {
		return nil, err
	}

	hit.Request.QueryParams = mergedParams

	return hit, nil
}

func (p *ga4Protocol) ClientID(request *hits.ParsedRequest) (string, error) {
	cid := request.QueryParams.Get("cid")
	if cid == "" {
		return "", errors.New("`cid` is a required query parameter for ga4 protocol")
	}
	return cid, nil
}

func (p *ga4Protocol) PropertyID(ctx *protocol.RequestContext) (string, error) {
	tid := ctx.Parsed.QueryParams.Get("tid")
	if tid == "" {
		return "", errors.New("`tid` is a required query parameter for ga4 protocol")
	}
	return p.propertyIDExtractor.PropertyID(ctx)
}

func (p *ga4Protocol) UserID(request *hits.ParsedRequest) (*string, error) {
	userID := request.QueryParams.Get("uid")
	if userID == "" {
		return nil, nil // nolint:nilnil // nil is valid for user ID
	}
	return &userID, nil
}

func (p *ga4Protocol) EventName(request *hits.ParsedRequest) (string, error) {
	eventName := request.QueryParams.Get("en")
	if eventName == "" {
		return "", errors.New("`en` is a required query parameter for ga4 protocol")
	}
	return eventName, nil
}

func (p *ga4Protocol) Interfaces() any {
	return ProtocolInterfaces
}

//go:embed static/gd.min.js
var staticDuplicatorJS []byte

//go:embed static/gd.min.js.map
var staticDuplicatorJSMap []byte

func (p *ga4Protocol) Endpoints() []protocol.ProtocolEndpoint {
	return []protocol.ProtocolEndpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Path:    "/g/collect",
		},
		{
			Methods:  []string{fasthttp.MethodGet},
			Path:     "/g/gd.min.js",
			IsCustom: true,
			CustomHandler: func(ctx *fasthttp.RequestCtx) {
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.Response.Header.Set("Content-Type", "text/javascript")
				ctx.SetBody(staticDuplicatorJS)
			},
		},
		{
			Methods:  []string{fasthttp.MethodGet},
			Path:     "/g/gd.min.js.map",
			IsCustom: true,
			CustomHandler: func(ctx *fasthttp.RequestCtx) {
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.Response.Header.Set("Content-Type", "application/json")
				ctx.SetBody(staticDuplicatorJSMap)
			},
		},
	}
}

func (p *ga4Protocol) Columns() schema.Columns { //nolint:funlen // contains all columns
	return schema.Columns{
		Event: []schema.EventColumn{
			eventMeasurementIDColumn,
			eventPageTitleColumn,
			eventPageReferrerColumn,
			eventPagePathColumn,
			eventPageLocationColumn,
			eventPageHostnameColumn,
			eventTrackingProtocolColumn,
			eventIgnoreReferrerColumn,
			eventIgnoreReferrerCoreColumn,
			eventPlatformColumn,
			eventEngagementTimeMsColumn,
			eventMethodColumn,
			eventCancellationReasonColumn,
			eventFatalColumn,
			genericEventParamsColumn,
			eventVideoCurrentTimeColumn,
			eventVideoDurationColumn,
			eventVideoPercentColumn,
			eventVideoProviderColumn,
			eventVideoTitleColumn,
			eventVideoURLColumn,
			eventFirebaseErrorColumn,
			eventFirebaseErrorValueColumn,
			eventFirebaseScreenColumn,
			eventFirebaseScreenClassColumn,
			eventFirebaseScreenIDColumn,
			eventFirebasePreviousScreenColumn,
			eventFirebasePreviousClassColumn,
			eventFirebasePreviousIDColumn,
			eventContentGroupColumn,
			eventContentIDColumn,
			eventContentTypeColumn,
			eventContentDescriptionColumn,
			eventCampaignColumn,
			eventCampaignIDColumn,
			eventCampaignSourceColumn,
			eventCampaignMediumColumn,
			eventCampaignContentColumn,
			eventCampaignTermColumn,
			eventAdEventIDColumn,
			eventExposureTimeColumn,
			eventAdUnitCodeColumn,
			eventRewardTypeColumn,
			eventRewardValueColumn,
			eventCouponColumn,
			eventCurrencyColumn,
			eventShippingColumn,
			eventShippingTierColumn,
			eventPaymentTypeColumn,
			eventParamTaxColumn,
			eventTransactionIDColumn,
			eventValueColumn,
			eventItemListIDColumn,
			eventItemListNameColumn,
			eventCreativeNameColumn,
			eventCreativeSlotColumn,
			eventPromotionIDColumn,
			eventPromotionNameColumn,
			eventLinkClassesColumn,
			eventLinkDomainColumn,
			eventLinkIDColumn,
			eventLinkTextColumn,
			eventLinkURLColumn,
			eventOutboundColumn,
			eventMessageDeviceTimeColumn,
			eventMessageIDColumn,
			eventMessageNameColumn,
			eventMessageTimeColumn,
			eventMessageTypeColumn,
			eventTopicColumn,
			eventLabelColumn,
			eventAppVersionColumn,
			eventPreviousAppVersionColumn,
			eventPreviousFirstOpenCountColumn,
			eventPreviousOSVersionColumn,
			eventUpdatedWithAnalyticsColumn,
			eventAchievementIDColumn,
			eventCharacterColumn,
			eventLevelColumn,
			eventLevelNameColumn,
			eventScoreColumn,
			eventVirtualCurrencyNameColumn,
			eventItemNameColumn,
			eventSuccessColumn,
			eventVisibleColumn,
			eventScreenResolutionColumn,
			eventSystemAppColumn,
			eventSystemAppUpdateColumn,
			eventDeferredAnalyticsCollectionColumn,
			eventResetAnalyticsCauseColumn,
			eventPreviousGmpAppIDColumn,
			eventFileExtensionColumn,
			eventFileNameColumn,
			eventFormDestinationColumn,
			eventGa4SessionIDColumn,
			eventGa4SessionNumberColumn,
			eventFormIDColumn,
			eventFormNameColumn,
			eventFormSubmitTextColumn,
			// Engagement params
			eventGroupIDColumn,
			eventLanguageColumn,
			eventPercentScrolledColumn,
			eventSearchTermColumn,
			// Lead params
			eventUnconvertLeadReasonColumn,
			eventDisqualifiedLeadReasonColumn,
			eventLeadSourceColumn,
			eventLeadStatusColumn,
			// E-commerce items
			itemsColumn(p.converter),
			// E-commerce params
			eventFreeTrialColumn,
			eventSubscriptionColumn,
			eventProductIDColumn,
			eventPriceColumn,
			eventQuantityColumn,
			eventIntroductoryPriceColumn,
			// E-commerce columns
			eventEcommercePurchaseRevenueColumn,
			eventEcommercePurchaseRevenueInUSDColumn(p.converter),
			eventEcommerceRefundValueColumn,
			eventEcommerceRefundValueInUSDColumn(p.converter),
			eventEcommerceShippingValueColumn,
			eventEcommerceShippingValueInUSDColumn(p.converter),
			eventEcommerceTaxValueColumn,
			eventEcommerceTaxValueInUSDColumn(p.converter),
			eventEcommerceUniqueItemsColumn,
			eventEcommerceItemsTotalQuantityColumn,
			// Page URL params
			gtmDebugColumn,
			// **lid params
			gclidParamColumn,
			dclidParamColumn,
			srsltidParamColumn,
			aclidParamColumn,
			anidParamColumn,
			renewalCountParamColumn,
			// Device columns
			deviceLanguageColumn,
			// Date columns
			eventTimestampUTCColumn,
			eventDateUTCColumn,
			eventPageLoadHashColumn,
			// Privacy columns
			eventPrivacyAnalyticsStorageColumn,
			eventPrivacyAdsStorageColumn,
			// GA Session columns
			gaSessionIDParamColumn,
			gaSessionNumberParamColumn,
		},
		SessionScopedEvent: []schema.SessionScopedEventColumn{
			eventPreviousPageLocationColumn,
			eventNextPageLocationColumn,
			eventPreviousPageTitleColumn,
			eventNextPageTitleColumn,
			sseTimeOnPageColumn,
			sseIsEntryPageColumn,
			sseIsExitPageColumn,
		},
		Session: []schema.SessionColumn{
			sessionEngagementColumn,
			sessionReturningUserColumn,
			sessionAbandonedCartColumn,
			sessionEntryPageLocationColumn,
			sessionExitPageLocationColumn,
			sessionEntryPageTitleColumn,
			sessionExitPageTitleColumn,
			sessionSecondPageLocationColumn,
			sessionSecondPageTitleColumn,
			sessionUtmMarketingTacticColumn,
			sessionUtmSourcePlatformColumn,
			sessionUtmTermColumn,
			sessionUtmContentColumn,
			sessionUtmSourceColumn,
			sessionUtmMediumColumn,
			sessionUtmCampaignColumn,
			sessionUtmIDColumn,
			sessionUtmCreativeFormatColumn,
			sessionClickIDGclidColumn,
			sessionClickIDDclidColumn,
			sessionClickIDSrsltidColumn,
			sessionClickIDGbraidColumn,
			sessionClickIDWbraidColumn,
			sessionClickIDFbclidColumn,
			sessionClickIDMsclkidColumn,
			sessionTotalPageViewsColumn,
			sessionUniquePageViewsColumn,
			sessionTotalPurchasesColumn,
			sessionTotalScrollsColumn,
			sessionTotalOutboundClicksColumn,
			sessionUniqueOutboundClicksColumn,
			sessionTotalSiteSearchesColumn,
			sessionUniqueSiteSearchesColumn,
			sessionTotalFormInteractionsColumn,
			sessionUniqueFormInteractionsColumn,
			sessionTotalVideoEngagementsColumn,
			sessionTotalFileDownloadsColumn,
			sessionUniqueFileDownloadsColumn,
		},
	}
}

// GA4ProtocolOption is an option function for configuring GA4Protocol.
type GA4ProtocolOption func(*ga4Protocol)

// WithPropertyIDExtractor sets a custom PropertyIDExtractor for the protocol.
func WithPropertyIDExtractor(extractor protocol.PropertyIDExtractor) GA4ProtocolOption {
	return func(p *ga4Protocol) {
		p.propertyIDExtractor = extractor
	}
}

type fromTidByMeasurementIDExtractor struct {
	psr properties.SettingsRegistry
}

func (e *fromTidByMeasurementIDExtractor) PropertyID(ctx *protocol.RequestContext) (string, error) {
	property, err := e.psr.GetByMeasurementID(ctx.Parsed.QueryParams.Get("tid"))
	if err != nil {
		return "", err
	}
	return property.PropertyID, nil
}

// NewFromTidByMeasurementIDExtractor creates a PropertyIDExtractor that extracts
// property ID from the "tid" query parameter using the property settings registry.
func NewFromTidByMeasurementIDExtractor(psr properties.SettingsRegistry) protocol.PropertyIDExtractor {
	return &fromTidByMeasurementIDExtractor{
		psr: psr,
	}
}

// NewGA4Protocol creates a new instance of the GA4 protocol handler.
func NewGA4Protocol(
	converter currency.Converter,
	psr properties.SettingsRegistry,
	opts ...GA4ProtocolOption,
) protocol.Protocol {
	p := &ga4Protocol{
		converter:           converter,
		propertyIDExtractor: NewFromTidByMeasurementIDExtractor(psr),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}
