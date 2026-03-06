package matomo

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint:funlen,lll // test code
func TestMatomoEventColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildDeterministicTimeHit := func(t *testing.T) *hits.Hit {
		hit := hits.New()
		hit.EventName = "page_view"
		hit.PropertyID = "test_property_id"
		warsaw, err := time.LoadLocation("Europe/Warsaw")
		require.NoError(t, err)
		// 1 AM Warsaw time = midnight UTC (Warsaw is UTC+1 in January)
		hit.MustParsedRequest().ServerReceivedTime = time.Date(2025, 1, 1, 1, 0, 0, 0, warsaw)
		return hit
	}

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "page_view"
		return hit
	}

	buildNonPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "scroll"
		return hit
	}

	single := func(build func(*testing.T) *hits.Hit) func(*testing.T) columntests.TestHits {
		return func(t *testing.T) columntests.TestHits {
			return columntests.TestHits{build(t)}
		}
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		expectNoIO  bool
		description string
	}

	mergeCases := func(groups ...[]testCase) []testCase {
		total := 0
		for _, g := range groups {
			total += len(g)
		}
		out := make([]testCase, 0, total)
		for _, g := range groups {
			out = append(out, g...)
		}
		return out
	}

	clickIDCases := func(fieldName, urlParam, clickIDValue string) []testCase {
		return []testCase{
			{
				name: fieldName + "_PageViewWithValue",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildPageViewHit(t)}
				},
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/?"+urlParam+"="+clickIDValue)},
				fieldName:   fieldName,
				expected:    clickIDValue,
				description: "Returns click ID value from the first page view event",
			},
			{
				name: fieldName + "_SecondHitIsPageViewWithValue",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildNonPageViewHit(t), buildPageViewHit(t)}
				},
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(1, "url", "https://example.com/?"+urlParam+"="+clickIDValue),
					columntests.EnsureQueryParam(1, "v", "2"),
				},
				fieldName:   fieldName,
				expected:    clickIDValue,
				description: "Returns click ID from the first page view even when preceded by a non-page-view event",
			},
			{
				name: fieldName + "_NoPageViewEvents",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildNonPageViewHit(t)}
				},
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/?"+urlParam+"="+clickIDValue)},
				fieldName:   fieldName,
				expected:    nil,
				description: "Returns nil when there are no page view events in the session",
			},
		}
	}

	testCases := mergeCases(
		[]testCase{
			{
				name:        "EventIgnoreReferrer_TrueViaReferrer",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ignore_referrer", "1")},
				fieldName:   "ignore_referrer",
				expected:    true,
				description: "ignore_referrer=1 returns true",
			},
			{
				name:        "EventIgnoreReferrer_TrueViaReferer",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ignore_referer", "1")},
				fieldName:   "ignore_referrer",
				expected:    true,
				description: "ignore_referer=1 (misspelled alias) returns true",
			},
			{
				name:        "EventIgnoreReferrer_FalseWhenZero",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ignore_referrer", "0")},
				fieldName:   "ignore_referrer",
				expected:    false,
				description: "ignore_referrer=0 returns false",
			},
			{
				name:        "EventIgnoreReferrer_NilWhenAbsent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "ignore_referrer",
				expected:    nil,
				description: "Returns nil when neither ignore_referrer nor ignore_referer is present",
			},
			{
				name:        "EventDateUTC_Valid",
				buildHits:   single(buildDeterministicTimeHit),
				fieldName:   "date_utc",
				expected:    "2025-01-01",
				description: "Valid event date UTC",
			},
			{
				name:        "EventTimestampUTC_Valid",
				buildHits:   single(buildDeterministicTimeHit),
				fieldName:   "timestamp_utc",
				expected:    "2025-01-01T00:00:00Z",
				description: "Valid event timestamp UTC",
			},
			{
				name:        "EventPageReferrer_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "urlref", "https://example.com")},
				fieldName:   "page_referrer",
				expected:    "https://example.com",
				description: "Valid page referrer via Matomo urlref parameter",
			},
			{
				name:        "EventPageReferrer_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "urlref", "")},
				fieldName:   "page_referrer",
				expected:    "",
				description: "Empty page referrer via Matomo urlref parameter",
			},
			{
				name:        "EventPageTitle_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "action_name", "My Page")},
				fieldName:   "page_title",
				expected:    "My Page",
				description: "Valid page title via Matomo action_name parameter",
			},
			{
				name:        "EventPageTitle_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "action_name", "")},
				fieldName:   "page_title",
				expected:    "",
				description: "Empty page title via Matomo action_name parameter",
			},
			{
				name:        "EventPageLocation_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/path?foo=bar")},
				fieldName:   "page_location",
				expected:    "https://example.com/path?foo=bar",
				description: "Valid page location via Matomo url parameter",
			},
			{
				name:        "EventPageLocation_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "")},
				fieldName:   "page_location",
				expected:    "",
				description: "Empty page location via Matomo url parameter",
			},
			{
				name:        "EventPageLocation_BrokenURL",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "://bad")},
				fieldName:   "page_location",
				expectNoIO:  true,
				description: "Broken page location results in filtered-out session/event (no warehouse writes)",
			},
			{
				name:        "EventPageHostname_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/path")},
				fieldName:   "page_hostname",
				expected:    "example.com",
				description: "Valid page hostname derived from page_location",
			},
			{
				name:        "EventPagePath_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/my/path")},
				fieldName:   "page_path",
				expected:    "/my/path",
				description: "Valid page path derived from page_location",
			},
			{
				name:        "EventTrackingProtocol",
				buildHits:   single(buildPageViewHit),
				fieldName:   "tracking_protocol",
				expected:    "matomo",
				description: "Tracking protocol is constant matomo",
			},
			{
				name:        "EventPlatform",
				buildHits:   single(buildPageViewHit),
				fieldName:   "platform",
				expected:    "web",
				description: "Platform is constant web",
			},
			{
				name:        "DeviceLanguage_ViaParam",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "lang", "fr-fr")},
				fieldName:   "device_language",
				expected:    "fr-fr",
				description: "Device language via lang query parameter",
			},
			{
				name:      "DeviceLanguage_ViaHeader",
				buildHits: single(buildPageViewHit),
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(0, "lang", ""),
					columntests.EnsureHeader(0, "Accept-Language", "de-de"),
				},
				fieldName:   "device_language",
				expected:    "de-de",
				description: "Device language via Accept-Language header fallback",
			},
		},
		[]testCase{
			{
				name:        "EventParamsCategory_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_c", "checkout")},
				fieldName:   "params_category",
				expected:    "checkout",
				description: "Valid params category via e_c query parameter",
			},
			{
				name:        "EventParamsCategory_Empty",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_category",
				expected:    nil,
				description: "Returns nil when e_c parameter is absent",
			},
			{
				name:        "EventParamsAction_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_a", "add_to_cart")},
				fieldName:   "params_action",
				expected:    "add_to_cart",
				description: "Valid params action via e_a query parameter",
			},
			{
				name:        "EventParamsAction_Empty",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_action",
				expected:    nil,
				description: "Returns nil when e_a parameter is absent",
			},
			{
				name:        "EventParamsValue_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "99.95")},
				fieldName:   "params_value",
				expected:    99.95,
				description: "Valid numeric params value via e_v query parameter",
			},
			{
				name:        "EventParamsValue_ValidInteger",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "42")},
				fieldName:   "params_value",
				expected:    42.0,
				description: "Valid integer params value converted to float64",
			},
			{
				name:        "EventParamsValue_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "not_a_number")},
				fieldName:   "params_value",
				expected:    nil,
				description: "Returns nil when e_v is not parseable as float64",
			},
			{
				name:        "EventParamsValue_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "")},
				fieldName:   "params_value",
				expected:    nil,
				description: "Returns nil when e_v parameter is empty",
			},
			{
				name:        "EventParamsValue_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_value",
				expected:    nil,
				description: "Returns nil when e_v parameter is absent",
			},
			{
				name:        "EventParamsContentInteraction_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_i", "click")},
				fieldName:   "params_content_interaction",
				expected:    "click",
				description: "Valid params content interaction via c_i query parameter",
			},
			{
				name:        "EventParamsContentInteraction_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_interaction",
				expected:    nil,
				description: "Returns nil when c_i parameter is absent",
			},
			{
				name:        "EventParamsContentName_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_n", "Hero Banner")},
				fieldName:   "params_content_name",
				expected:    "Hero Banner",
				description: "Valid params content name via c_n query parameter",
			},
			{
				name:        "EventParamsContentName_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_name",
				expected:    nil,
				description: "Returns nil when c_n parameter is absent",
			},
			{
				name:        "EventParamsContentPiece_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_p", "/assets/banner.jpg")},
				fieldName:   "params_content_piece",
				expected:    "/assets/banner.jpg",
				description: "Valid params content piece via c_p query parameter",
			},
			{
				name:        "EventParamsContentPiece_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_piece",
				expected:    nil,
				description: "Returns nil when c_p parameter is absent",
			},
			{
				name:        "EventParamsContentTarget_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_t", "https://example.com/landing")},
				fieldName:   "params_content_target",
				expected:    "https://example.com/landing",
				description: "Valid params content target via c_t query parameter",
			},
			{
				name:        "EventParamsContentTarget_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_target",
				expected:    nil,
				description: "Returns nil when c_t parameter is absent",
			},
			{
				name:        "EventParamsProductPrice_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkp", "129.99")},
				fieldName:   "params_product_price",
				expected:    129.99,
				description: "Valid product price via _pkp query parameter",
			},
			{
				name:        "EventParamsProductPrice_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkp", "not_a_number")},
				fieldName:   "params_product_price",
				expected:    nil,
				description: "Returns nil when _pkp is not parseable as float64",
			},
			{
				name:        "EventParamsProductPrice_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_product_price",
				expected:    nil,
				description: "Returns nil when _pkp parameter is absent",
			},
			{
				name:        "EventEcommercePurchaseRevenue_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "revenue", "249.50")},
				fieldName:   "ecommerce_purchase_revenue",
				expected:    249.50,
				description: "Valid ecommerce purchase revenue via revenue query parameter",
			},
			{
				name:        "EventEcommercePurchaseRevenue_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "revenue", "not_a_number")},
				fieldName:   "ecommerce_purchase_revenue",
				expected:    nil,
				description: "Returns nil when revenue is not parseable as float64",
			},
			{
				name:        "EventEcommercePurchaseRevenue_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "ecommerce_purchase_revenue",
				expected:    nil,
				description: "Returns nil when revenue parameter is absent",
			},
			{
				name:        "EventEcommerceShippingValue_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_sh", "14.99")},
				fieldName:   "ecommerce_shipping_value",
				expected:    14.99,
				description: "Valid ecommerce shipping value via ec_sh query parameter",
			},
			{
				name:        "EventEcommerceShippingValue_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_sh", "not_a_number")},
				fieldName:   "ecommerce_shipping_value",
				expected:    nil,
				description: "Returns nil when ec_sh is not parseable as float64",
			},
			{
				name:        "EventEcommerceShippingValue_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "ecommerce_shipping_value",
				expected:    nil,
				description: "Returns nil when ec_sh parameter is absent",
			},
			{
				name:        "EventEcommerceSubtotalValue_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_st", "199.99")},
				fieldName:   "ecommerce_subtotal_value",
				expected:    199.99,
				description: "Valid ecommerce subtotal value via ec_st query parameter",
			},
			{
				name:        "EventEcommerceSubtotalValue_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_st", "not_a_number")},
				fieldName:   "ecommerce_subtotal_value",
				expected:    nil,
				description: "Returns nil when ec_st is not parseable as float64",
			},
			{
				name:        "EventEcommerceSubtotalValue_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "ecommerce_subtotal_value",
				expected:    nil,
				description: "Returns nil when ec_st parameter is absent",
			},
			{
				name:        "EventEcommerceTaxValue_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_tx", "20.75")},
				fieldName:   "ecommerce_tax_value",
				expected:    20.75,
				description: "Valid ecommerce tax value via ec_tx query parameter",
			},
			{
				name:        "EventEcommerceTaxValue_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_tx", "not_a_number")},
				fieldName:   "ecommerce_tax_value",
				expected:    nil,
				description: "Returns nil when ec_tx is not parseable as float64",
			},
			{
				name:        "EventEcommerceTaxValue_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "ecommerce_tax_value",
				expected:    nil,
				description: "Returns nil when ec_tx parameter is absent",
			},
			{
				name:        "EventEcommerceDiscountValue_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_dt", "12.00")},
				fieldName:   "ecommerce_discount_value",
				expected:    12.0,
				description: "Valid ecommerce discount value via ec_dt query parameter",
			},
			{
				name:        "EventEcommerceDiscountValue_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_dt", "not_a_number")},
				fieldName:   "ecommerce_discount_value",
				expected:    nil,
				description: "Returns nil when ec_dt is not parseable as float64",
			},
			{
				name:        "EventEcommerceDiscountValue_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "ecommerce_discount_value",
				expected:    nil,
				description: "Returns nil when ec_dt parameter is absent",
			},
			{
				name:        "EventParamsProductSKU_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pks", "SKU-123")},
				fieldName:   "params_product_sku",
				expected:    "SKU-123",
				description: "Valid product SKU via _pks query parameter",
			},
			{
				name:        "EventParamsProductSKU_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_product_sku",
				expected:    nil,
				description: "Returns nil when _pks parameter is absent",
			},
			{
				name:        "EventParamsProductName_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkn", "Running Shoes")},
				fieldName:   "params_product_name",
				expected:    "Running Shoes",
				description: "Valid product name via _pkn query parameter",
			},
			{
				name:        "EventParamsProductName_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_product_name",
				expected:    nil,
				description: "Returns nil when _pkn parameter is absent",
			},
			{
				name:        "EventParamsProductCategory1_ValidString",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "Shoes")},
				fieldName:   "params_product_category_1",
				expected:    "Shoes",
				description: "String _pkc maps to first flattened category",
			},
			{
				name:      "EventParamsProductCategory1_ValidJSONList",
				buildHits: single(buildPageViewHit),
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(0, "_pkc", `[
						"Men",
						"Shoes",
						"Running",
						"Sneakers",
						"Sale",
						"Ignored"
					]`),
				},
				fieldName:   "params_product_category_1",
				expected:    "Men",
				description: "First JSON category maps to category_1",
			},
			{
				name:      "EventParamsProductCategory2_ValidJSONList",
				buildHits: single(buildPageViewHit),
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(0, "_pkc", `[
						"Men",
						"Shoes"
					]`),
				},
				fieldName:   "params_product_category_2",
				expected:    "Shoes",
				description: "Second JSON category maps to category_2",
			},
			{
				name:      "EventParamsProductCategory3_ValidJSONList",
				buildHits: single(buildPageViewHit),
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(0, "_pkc", `[
						"Men",
						"Shoes",
						"Running"
					]`),
				},
				fieldName:   "params_product_category_3",
				expected:    "Running",
				description: "Third JSON category maps to category_3",
			},
			{
				name:      "EventParamsProductCategory4_ValidJSONList",
				buildHits: single(buildPageViewHit),
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(0, "_pkc", `[
						"Men",
						"Shoes",
						"Running",
						"Sneakers"
					]`),
				},
				fieldName:   "params_product_category_4",
				expected:    "Sneakers",
				description: "Fourth JSON category maps to category_4",
			},
			{
				name:      "EventParamsProductCategory5_ValidJSONList",
				buildHits: single(buildPageViewHit),
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(0, "_pkc", `[
						"Men",
						"Shoes",
						"Running",
						"Sneakers",
						"Sale",
						"Ignored"
					]`),
				},
				fieldName:   "params_product_category_5",
				expected:    "Sale",
				description: "Fifth JSON category maps to category_5 and ignores extras",
			},
			{
				name:        "EventParamsProductCategory2_ValidString",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "Shoes")},
				fieldName:   "params_product_category_2",
				expected:    nil,
				description: "Single string _pkc leaves category_2 nil",
			},
			{
				name:        "EventParamsProductCategory1_EmptyJSONList",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "[]")},
				fieldName:   "params_product_category_1",
				expected:    nil,
				description: "Returns nil when _pkc is an empty JSON list",
			},
			{
				name:        "EventParamsProductCategory1_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "")},
				fieldName:   "params_product_category_1",
				expected:    nil,
				description: "Returns nil when _pkc parameter is empty",
			},
			{
				name:        "EventParamsProductCategory1_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_product_category_1",
				expected:    nil,
				description: "Returns nil when _pkc parameter is absent",
			},
		},
		clickIDCases("session_click_id_gclid", "gclid", "gclid_test_123"),
		clickIDCases("session_click_id_dclid", "dclid", "dclid_test_123"),
		clickIDCases("session_click_id_gbraid", "gbraid", "gbraid_test_123"),
		clickIDCases("session_click_id_srsltid", "srsltid", "srsltid_test_123"),
		clickIDCases("session_click_id_wbraid", "wbraid", "wbraid_test_123"),
		clickIDCases("session_click_id_fbclid", "fbclid", "fbclid_test_123"),
		clickIDCases("session_click_id_msclkid", "msclkid", "msclkid_test_123"),
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					if tc.expectNoIO {
						require.Empty(t, whd.WriteCalls, "expected no warehouse write calls")
						return
					}
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record written")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}
