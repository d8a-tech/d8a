package ga4

import (
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
)

type ga4Protocol struct {
	converter currency.Converter
}

func (p *ga4Protocol) ID() string {
	return "ga4"
}

func (p *ga4Protocol) Hits(request *protocol.Request) ([]*hits.Hit, error) {
	// Read the request body
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}

	// Parse body into lines (each line represents a hit)
	bodyStr := strings.TrimSpace(string(body))
	var bodyLines []string
	if bodyStr != "" {
		bodyLines = strings.Split(bodyStr, "\n")
	}

	// If no body lines, create one hit with just query parameters
	if len(bodyLines) == 0 {
		hit, err := p.createHitFromQueryParams(request, body)
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

		hit, err := p.createHitFromLine(request, line, body)
		if err != nil {
			return nil, err
		}
		theHits = append(theHits, hit)
	}

	return theHits, nil
}

// createHitFromQueryParams creates a hit using only query parameters
func (p *ga4Protocol) createHitFromQueryParams(request *protocol.Request, body []byte) (*hits.Hit, error) {
	hit := hits.New()

	clientID, err := p.ClientID(request)
	if err != nil {
		return nil, err
	}
	hit.ClientID = hits.ClientID(clientID)
	hit.AuthoritativeClientID = hit.ClientID

	hit.PropertyID, err = p.PropertyID(request)
	if err != nil {
		return nil, err
	}

	timestamp, err := p.Timestamp(request)
	if err != nil {
		return nil, err
	}
	hit.Timestamp = timestamp

	hit.UserID, err = p.UserID(request)
	if err != nil {
		return nil, err
	}

	hit.Body = body
	hit.Host = string(request.Host)
	hit.Path = string(request.Path)
	hit.Method = string(request.Method)

	headers := url.Values{}
	for key, values := range request.Headers {
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	hit.Headers = headers

	queryParams := url.Values{}
	for key, values := range request.QueryParams {
		for _, value := range values {
			queryParams.Add(key, value)
		}
	}
	hit.QueryParams = queryParams

	return hit, nil
}

// createHitFromLine creates a hit by merging query parameters with line-specific parameters
func (p *ga4Protocol) createHitFromLine(request *protocol.Request, line string, body []byte) (*hits.Hit, error) {
	mergedParams, err := p.mergeQueryParamsWithLine(request.QueryParams, line)
	if err != nil {
		return nil, err
	}

	// Create a temporary request with merged parameters for extracting common fields
	tempRequest := &protocol.Request{
		QueryParams: mergedParams,
		Headers:     request.Headers,
		Host:        request.Host,
		Path:        request.Path,
		Method:      request.Method,
		Body:        request.Body,
	}

	hit, err := p.createHitFromMergedParams(tempRequest, body, mergedParams)
	if err != nil {
		return nil, err
	}

	return hit, nil
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
	request *protocol.Request,
	body []byte,
	mergedParams url.Values,
) (*hits.Hit, error) {
	hit := hits.New()

	clientID, err := p.ClientID(request)
	if err != nil {
		return nil, err
	}
	hit.ClientID = hits.ClientID(clientID)
	hit.AuthoritativeClientID = hit.ClientID

	hit.PropertyID, err = p.PropertyID(request)
	if err != nil {
		return nil, err
	}

	timestamp, err := p.Timestamp(request)
	if err != nil {
		return nil, err
	}
	hit.Timestamp = timestamp

	hit.UserID, err = p.UserID(request)
	if err != nil {
		return nil, err
	}

	hit.Body = body
	hit.Host = string(request.Host)
	hit.Path = string(request.Path)
	hit.Method = string(request.Method)

	headers := url.Values{}
	for key, values := range request.Headers {
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	hit.Headers = headers

	hit.QueryParams = mergedParams

	return hit, nil
}

func (p *ga4Protocol) ClientID(request *protocol.Request) (string, error) {
	cid := request.QueryParams.Get("cid")
	if cid == "" {
		return "", errors.New("`cid` is a required query parameter for ga4 protocol")
	}
	return cid, nil
}

func (p *ga4Protocol) PropertyID(request *protocol.Request) (string, error) {
	propertyID := request.QueryParams.Get("tid")
	if propertyID == "" {
		return "", errors.New("`tid` is a required query parameter for ga4 protocol")
	}
	return propertyID, nil
}

func (p *ga4Protocol) Timestamp(request *protocol.Request) (time.Time, error) {
	timestamp := request.QueryParams.Get("_p")
	if timestamp == "" {
		return time.Time{}, errors.New("`_p` is a required query parameter for ga4 protocol")
	}
	timestampInt, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(timestampInt/1000, (timestampInt%1000)*1000000), nil
}

func (p *ga4Protocol) UserID(request *protocol.Request) (*string, error) {
	userID := request.QueryParams.Get("uid")
	if userID == "" {
		return nil, nil // nolint:nilnil // nil is valid for user ID
	}
	return &userID, nil
}

func (p *ga4Protocol) Columns() schema.Columns { //nolint:funlen // contains all columns
	return schema.Columns{
		Event: []schema.EventColumn{
			eventNameColumn,
			eventPageTitleColumn,
			eventPageReferrerColumn,
			eventPagePathColumn,
			eventPageLocationColumn,
			eventPageHostnameColumn,
			eventTrackingProtocolColumn,
			eventIgnoreReferrerColumn,
			eventPlatformColumn,
			eventEngagementTimeMsColumn,
			eventMethodColumn,
			eventCancellationReasonColumn,
			eventFatalColumn,
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
			eventTaxColumn,
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
			itemsColumn(p.converter),
			eventFreeTrialColumn,
			eventSubscriptionColumn,
			eventPurchaseRevenueColumn,
			eventPurchaseRevenueInUSDColumn(p.converter),
			eventRefundValueColumn,
			eventRefundValueInUSDColumn(p.converter),
			eventShippingValueColumn,
			eventShippingValueInUSDColumn(p.converter),
			eventTaxValueInUSDColumn(p.converter),
			eventUniqueItemsColumn,
			eventItemsTotalQuantityColumn,
			// Page URL params
			gtmDebugColumn,
			glColumn,
			// **lid params
			gclidParamColumn,
			dclidParamColumn,
			srsltidParamColumn,
			aclidParamColumn,
			anidParamColumn,
			// Click ids params
			clickIDsGclidColumn,
			clickIDsDclidColumn,
			clickIDsSrsltidColumn,
			clickIDsGbraidColumn,
			clickIDsWbraidColumn,
			clickIDsMsclkidColumn,
		},
		Session: []schema.SessionColumn{
			sessionGa4SessionIDColumn,
			sessionNumberColumn,
			sessionEngagementColumn,
		},
	}
}

// NewGA4Protocol creates a new instance of the GA4 protocol handler.
func NewGA4Protocol(converter currency.Converter) protocol.Protocol {
	return &ga4Protocol{
		converter: converter,
	}
}
