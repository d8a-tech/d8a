package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoProductColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) columntests.TestHits {
		hit := columntests.TestHitOne()
		hit.EventName = pageViewEventType
		return columntests.TestHits{hit}
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		description string
	}

	testCases := []testCase{
		{
			name:        "EventParamsProductPrice_ValidNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkp", "129.99")},
			fieldName:   "params_product_price",
			expected:    129.99,
			description: "Valid product price via _pkp query parameter",
		},
		{
			name:        "EventParamsProductPrice_NonNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkp", "not_a_number")},
			fieldName:   "params_product_price",
			expected:    nil,
			description: "Returns nil when _pkp is not parseable as float64",
		},
		{
			name:        "EventParamsProductPrice_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "params_product_price",
			expected:    nil,
			description: "Returns nil when _pkp parameter is absent",
		},
		{
			name:        "EventEcommercePurchaseRevenue_ValidNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "revenue", "249.50")},
			fieldName:   "ecommerce_purchase_revenue",
			expected:    249.50,
			description: "Valid ecommerce purchase revenue via revenue query parameter",
		},
		{
			name:        "EventEcommercePurchaseRevenue_NonNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "revenue", "not_a_number")},
			fieldName:   "ecommerce_purchase_revenue",
			expected:    nil,
			description: "Returns nil when revenue is not parseable as float64",
		},
		{
			name:        "EventEcommercePurchaseRevenue_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "ecommerce_purchase_revenue",
			expected:    nil,
			description: "Returns nil when revenue parameter is absent",
		},
		{
			name:        "EventEcommerceShippingValue_ValidNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_sh", "14.99")},
			fieldName:   "ecommerce_shipping_value",
			expected:    14.99,
			description: "Valid ecommerce shipping value via ec_sh query parameter",
		},
		{
			name:        "EventEcommerceShippingValue_NonNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_sh", "not_a_number")},
			fieldName:   "ecommerce_shipping_value",
			expected:    nil,
			description: "Returns nil when ec_sh is not parseable as float64",
		},
		{
			name:        "EventEcommerceShippingValue_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "ecommerce_shipping_value",
			expected:    nil,
			description: "Returns nil when ec_sh parameter is absent",
		},
		{
			name:        "EventEcommerceSubtotalValue_ValidNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_st", "199.99")},
			fieldName:   "ecommerce_subtotal_value",
			expected:    199.99,
			description: "Valid ecommerce subtotal value via ec_st query parameter",
		},
		{
			name:        "EventEcommerceSubtotalValue_NonNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_st", "not_a_number")},
			fieldName:   "ecommerce_subtotal_value",
			expected:    nil,
			description: "Returns nil when ec_st is not parseable as float64",
		},
		{
			name:        "EventEcommerceSubtotalValue_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "ecommerce_subtotal_value",
			expected:    nil,
			description: "Returns nil when ec_st parameter is absent",
		},
		{
			name:        "EventEcommerceTaxValue_ValidNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_tx", "20.75")},
			fieldName:   "ecommerce_tax_value",
			expected:    20.75,
			description: "Valid ecommerce tax value via ec_tx query parameter",
		},
		{
			name:        "EventEcommerceTaxValue_NonNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_tx", "not_a_number")},
			fieldName:   "ecommerce_tax_value",
			expected:    nil,
			description: "Returns nil when ec_tx is not parseable as float64",
		},
		{
			name:        "EventEcommerceTaxValue_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "ecommerce_tax_value",
			expected:    nil,
			description: "Returns nil when ec_tx parameter is absent",
		},
		{
			name:        "EventEcommerceDiscountValue_ValidNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_dt", "12.00")},
			fieldName:   "ecommerce_discount_value",
			expected:    12.0,
			description: "Valid ecommerce discount value via ec_dt query parameter",
		},
		{
			name:        "EventEcommerceDiscountValue_NonNumeric",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_dt", "not_a_number")},
			fieldName:   "ecommerce_discount_value",
			expected:    nil,
			description: "Returns nil when ec_dt is not parseable as float64",
		},
		{
			name:        "EventEcommerceDiscountValue_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "ecommerce_discount_value",
			expected:    nil,
			description: "Returns nil when ec_dt parameter is absent",
		},
		{
			name:        "EventParamsProductSKU_Valid",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pks", "SKU-123")},
			fieldName:   "params_product_sku",
			expected:    "SKU-123",
			description: "Valid product SKU via _pks query parameter",
		},
		{
			name:        "EventParamsProductSKU_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "params_product_sku",
			expected:    nil,
			description: "Returns nil when _pks parameter is absent",
		},
		{
			name:        "EventParamsProductName_Valid",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkn", "Running Shoes")},
			fieldName:   "params_product_name",
			expected:    "Running Shoes",
			description: "Valid product name via _pkn query parameter",
		},
		{
			name:        "EventParamsProductName_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "params_product_name",
			expected:    nil,
			description: "Returns nil when _pkn parameter is absent",
		},
		{
			name:        "EventParamsProductCategory1_ValidString",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "Shoes")},
			fieldName:   "params_product_category_1",
			expected:    "Shoes",
			description: "String _pkc maps to first flattened category",
		},
		{
			name:      "EventParamsProductCategory1_ValidJSONList",
			buildHits: buildPageViewHit,
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
			buildHits: buildPageViewHit,
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
			buildHits: buildPageViewHit,
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
			buildHits: buildPageViewHit,
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
			buildHits: buildPageViewHit,
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
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "Shoes")},
			fieldName:   "params_product_category_2",
			expected:    nil,
			description: "Single string _pkc leaves category_2 nil",
		},
		{
			name:        "EventParamsProductCategory1_EmptyJSONList",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "[]")},
			fieldName:   "params_product_category_1",
			expected:    nil,
			description: "Returns nil when _pkc is an empty JSON list",
		},
		{
			name:        "EventParamsProductCategory1_Empty",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "_pkc", "")},
			fieldName:   "params_product_category_1",
			expected:    nil,
			description: "Returns nil when _pkc parameter is empty",
		},
		{
			name:        "EventParamsProductCategory1_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "params_product_category_1",
			expected:    nil,
			description: "Returns nil when _pkc parameter is absent",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
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
