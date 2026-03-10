package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/schema"
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
		assertValue func(t *testing.T, actual any)
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
		{
			name:        "EventEcommerceOrderID_Valid",
			buildHits:   buildPageViewHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ec_id", "ORD-12345")},
			fieldName:   "ecommerce_order_id",
			expected:    "ORD-12345",
			description: "Valid ecommerce order ID via ec_id query parameter",
		},
		{
			name:        "EventEcommerceOrderID_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   "ecommerce_order_id",
			expected:    nil,
			description: "Returns nil when ec_id parameter is absent",
		},
		{
			name:      "EventEcommerceItems_OneItem",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "ec_items", `[["SKU-1","Item 1","Category 1",12.34,2]]`),
			},
			fieldName: ProtocolInterfaces.EventEcommerceItems.Field.Name,
			expected: []any{
				map[string]any{
					ecommerceSKU:       "SKU-1",
					ecommerceName:      "Item 1",
					ecommerceCategory1: "Category 1",
					ecommercePrice:     12.34,
					ecommerceQuantity:  2.0,
				},
			},
			assertValue: func(t *testing.T, actual any) {
				expected := []any{
					map[string]any{
						ecommerceSKU:       "SKU-1",
						ecommerceName:      "Item 1",
						ecommerceCategory1: "Category 1",
						ecommercePrice:     12.34,
						ecommerceQuantity:  2.0,
					},
				}

				rows, ok := actual.([]map[string]any)
				require.True(t, ok)
				actualRows := make([]any, 0, len(rows))
				for _, row := range rows {
					actualRows = append(actualRows, row)
				}

				assert.Equal(t, expected, actualRows)
			},
			description: "One ec_items tuple maps to one nested ecommerce item row",
		},
		{
			name:      "EventEcommerceItems_MultiItem",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(
					0,
					"ec_items",
					`[["SKU-1","Item 1","Category 1",12.34,2],["SKU-2","Item 2","Category 2",56.78,3]]`,
				),
			},
			fieldName: ProtocolInterfaces.EventEcommerceItems.Field.Name,
			expected: []any{
				map[string]any{
					ecommerceSKU:       "SKU-1",
					ecommerceName:      "Item 1",
					ecommerceCategory1: "Category 1",
					ecommercePrice:     12.34,
					ecommerceQuantity:  2.0,
				},
				map[string]any{
					ecommerceSKU:       "SKU-2",
					ecommerceName:      "Item 2",
					ecommerceCategory1: "Category 2",
					ecommercePrice:     56.78,
					ecommerceQuantity:  3.0,
				},
			},
			assertValue: func(t *testing.T, actual any) {
				expected := []any{
					map[string]any{
						ecommerceSKU:       "SKU-1",
						ecommerceName:      "Item 1",
						ecommerceCategory1: "Category 1",
						ecommercePrice:     12.34,
						ecommerceQuantity:  2.0,
					},
					map[string]any{
						ecommerceSKU:       "SKU-2",
						ecommerceName:      "Item 2",
						ecommerceCategory1: "Category 2",
						ecommercePrice:     56.78,
						ecommerceQuantity:  3.0,
					},
				}

				rows, ok := actual.([]map[string]any)
				require.True(t, ok)
				actualRows := make([]any, 0, len(rows))
				for _, row := range rows {
					actualRows = append(actualRows, row)
				}

				assert.Equal(t, expected, actualRows)
			},
			description: "Multiple ec_items tuples map to multiple nested rows",
		},
		{
			name:      "EventEcommerceItems_CategoryString",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "ec_items", `[["SKU-1","Item 1","Category 1",12.34,2]]`),
			},
			fieldName: ProtocolInterfaces.EventEcommerceItems.Field.Name,
			expected: []any{
				map[string]any{
					ecommerceSKU:       "SKU-1",
					ecommerceName:      "Item 1",
					ecommerceCategory1: "Category 1",
					ecommercePrice:     12.34,
					ecommerceQuantity:  2.0,
				},
			},
			assertValue: func(t *testing.T, actual any) {
				expected := []any{
					map[string]any{
						ecommerceSKU:       "SKU-1",
						ecommerceName:      "Item 1",
						ecommerceCategory1: "Category 1",
						ecommercePrice:     12.34,
						ecommerceQuantity:  2.0,
					},
				}

				rows, ok := actual.([]map[string]any)
				require.True(t, ok)
				actualRows := make([]any, 0, len(rows))
				for _, row := range rows {
					actualRows = append(actualRows, row)
				}

				assert.Equal(t, expected, actualRows)
			},
			description: "String category maps only to category_1 in nested row",
		},
		{
			name:      "EventEcommerceItems_CategoryArray",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(
					0,
					"ec_items",
					`[["SKU-1","Item 1",["Cat1","Cat2","Cat3","Cat4","Cat5","Ignored"],12.34,2]]`,
				),
			},
			fieldName: ProtocolInterfaces.EventEcommerceItems.Field.Name,
			expected: []any{
				map[string]any{
					ecommerceSKU:       "SKU-1",
					ecommerceName:      "Item 1",
					ecommerceCategory1: "Cat1",
					ecommerceCategory2: "Cat2",
					ecommerceCategory3: "Cat3",
					ecommerceCategory4: "Cat4",
					ecommerceCategory5: "Cat5",
					ecommercePrice:     12.34,
					ecommerceQuantity:  2.0,
				},
			},
			assertValue: func(t *testing.T, actual any) {
				expected := []any{
					map[string]any{
						ecommerceSKU:       "SKU-1",
						ecommerceName:      "Item 1",
						ecommerceCategory1: "Cat1",
						ecommerceCategory2: "Cat2",
						ecommerceCategory3: "Cat3",
						ecommerceCategory4: "Cat4",
						ecommerceCategory5: "Cat5",
						ecommercePrice:     12.34,
						ecommerceQuantity:  2.0,
					},
				}

				rows, ok := actual.([]map[string]any)
				require.True(t, ok)
				actualRows := make([]any, 0, len(rows))
				for _, row := range rows {
					actualRows = append(actualRows, row)
				}

				assert.Equal(t, expected, actualRows)
			},
			description: "Array category maps to category_1 through category_5",
		},
		{
			name:      "EventEcommerceItems_DefaultOptionalTupleValues",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "ec_items", `[["SKU-1"]]`),
			},
			fieldName: ProtocolInterfaces.EventEcommerceItems.Field.Name,
			expected: []any{
				map[string]any{
					ecommerceSKU:       "SKU-1",
					ecommerceName:      "",
					ecommerceCategory1: "",
					ecommercePrice:     0.0,
					ecommerceQuantity:  1.0,
				},
			},
			assertValue: func(t *testing.T, actual any) {
				expected := []any{
					map[string]any{
						ecommerceSKU:       "SKU-1",
						ecommerceName:      "",
						ecommerceCategory1: "",
						ecommercePrice:     0.0,
						ecommerceQuantity:  1.0,
					},
				}

				rows, ok := actual.([]map[string]any)
				require.True(t, ok)
				actualRows := make([]any, 0, len(rows))
				for _, row := range rows {
					actualRows = append(actualRows, row)
				}

				assert.Equal(t, expected, actualRows)
			},
			description: "Missing optional tuple values default to empty name/category and 0/1 numbers",
		},
		{
			name:        "EventEcommerceItems_Absent",
			buildHits:   buildPageViewHit,
			fieldName:   ProtocolInterfaces.EventEcommerceItems.Field.Name,
			expected:    nil,
			description: "Returns nil when ec_items parameter is absent",
		},
		{
			name:        "EventEcommerceItems_EmptyArray",
			buildHits:   buildPageViewHit,
			fieldName:   ProtocolInterfaces.EventEcommerceItems.Field.Name,
			expected:    []map[string]any{},
			description: "Returns empty non-nil array when ec_items is explicit empty JSON array",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "ec_items", `[]`),
			},
		},
		{
			name:      "EventEcommerceItemsTotalQuantity_MultiItem",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(
					0,
					"ec_items",
					`[["SKU-1","Item 1","Category 1",12.34,2],["SKU-2","Item 2","Category 2",56.78,3]]`,
				),
			},
			fieldName:   ProtocolInterfaces.EventEcommerceItemsTotalQuantity.Field.Name,
			expected:    int64(5),
			description: "Sums quantity values across all ecommerce_items rows",
		},
		{
			name:      "EventEcommerceItemsTotalQuantity_DefaultQuantityContributes",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(
					0,
					"ec_items",
					`[["SKU-1","Item 1","Category 1",12.34],["SKU-2","Item 2","Category 2",56.78,2]]`,
				),
			},
			fieldName:   ProtocolInterfaces.EventEcommerceItemsTotalQuantity.Field.Name,
			expected:    int64(3),
			description: "Default quantity of 1 contributes when tuple quantity is omitted",
		},
		{
			name:        "EventEcommerceItemsTotalQuantity_AbsentEcItems",
			buildHits:   buildPageViewHit,
			fieldName:   ProtocolInterfaces.EventEcommerceItemsTotalQuantity.Field.Name,
			expected:    int64(0),
			description: "Returns 0 when ec_items parameter is absent",
		},
		{
			name:      "EventEcommerceColumns_CoexistOnOrderHit",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "ec_id", "ORD-COEXIST-1"),
				columntests.EnsureQueryParam(
					0,
					"ec_items",
					`[["SKU-1","Item 1","Category 1",12.34,2],["SKU-2","Item 2","Category 2",56.78,1]]`,
				),
			},
			fieldName: ProtocolInterfaces.EventEcommerceItemsTotalQuantity.Field.Name,
			assertValue: func(t *testing.T, actual any) {
				require.Equal(t, int64(3), actual)
			},
			description: "Order ID, ecommerce_items and total quantity are emitted together on same hit",
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
					if tc.assertValue != nil {
						tc.assertValue(t, record[tc.fieldName])
						return
					}

					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}

func TestMatomoProductColumns_EcommerceColumnsCoexistOnOrderHit(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})
	hit := columntests.TestHitOne()
	hit.EventName = pageViewEventType

	columntests.ColumnTestCase(
		t,
		columntests.TestHits{hit},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			require.NoError(t, closeErr)
			require.NotEmpty(t, whd.WriteCalls)
			require.NotEmpty(t, whd.WriteCalls[0].Records)

			record := whd.WriteCalls[0].Records[0]
			assert.Equal(t, "ORD-12345", record[ProtocolInterfaces.EventEcommerceOrderID.Field.Name])

			items, ok := record[ProtocolInterfaces.EventEcommerceItems.Field.Name].([]map[string]any)
			require.True(t, ok)
			require.Len(t, items, 2)
			assert.Equal(t, "SKU-1", items[0][ecommerceSKU])
			assert.Equal(t, float64(2), items[0][ecommerceQuantity])
			assert.Equal(t, "SKU-2", items[1][ecommerceSKU])
			assert.Equal(t, float64(1), items[1][ecommerceQuantity])

			assert.Equal(t, int64(3), record[ProtocolInterfaces.EventEcommerceItemsTotalQuantity.Field.Name])
		},
		proto,
		columntests.EnsureQueryParam(0, "v", "2"),
		columntests.EnsureQueryParam(0, "ec_id", "ORD-12345"),
		columntests.EnsureQueryParam(
			0,
			"ec_items",
			`[["SKU-1","Item 1","Category 1",12.34,2],["SKU-2","Item 2","Category 2",56.78,1]]`,
		),
	)
}

func TestMatomoProductColumns_EcommerceItemsMalformed(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})
	hit := columntests.TestHitOne()
	hit.EventName = pageViewEventType

	columntests.ColumnTestCase(
		t,
		columntests.TestHits{hit},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			assert.True(t, closeErr != nil || len(whd.WriteCalls) == 0)
		},
		proto,
		columntests.EnsureQueryParam(0, "v", "2"),
		columntests.EnsureQueryParam(0, "ec_items", `invalid json`),
	)
}

func TestParseEcommerceItems(t *testing.T) {
	type testCase struct {
		name        string
		raw         string
		expected    []map[string]any
		expectError bool
		description string
	}

	testCases := []testCase{
		{
			name: "ValidSingleItem",
			raw:  `[["SKU123", "Product Name", "Category", 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product Name",
					ecommerceCategory1: "Category",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Parses single item tuple correctly",
		},
		{
			name: "ValidMultipleItems",
			raw:  `[["SKU1", "Product 1", "Cat1", 10.0, 1], ["SKU2", "Product 2", "Cat2", 20.0, 3]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU1",
					ecommerceName:      "Product 1",
					ecommerceCategory1: "Cat1",
					ecommercePrice:     10.0,
					ecommerceQuantity:  1.0,
				},
				{
					ecommerceSKU:       "SKU2",
					ecommerceName:      "Product 2",
					ecommerceCategory1: "Cat2",
					ecommercePrice:     20.0,
					ecommerceQuantity:  3.0,
				},
			},
			expectError: false,
			description: "Parses multiple items correctly",
		},
		{
			name: "DefaultMissingName",
			raw:  `[["SKU123", null, "Category", 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "",
					ecommerceCategory1: "Category",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Defaults missing name to empty string",
		},
		{
			name: "DefaultMissingPrice",
			raw:  `[["SKU123", "Product Name", "Category", null, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product Name",
					ecommerceCategory1: "Category",
					ecommercePrice:     0.0,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Defaults missing price to 0",
		},
		{
			name: "DefaultMissingQuantity",
			raw:  `[["SKU123", "Product Name", "Category", 19.99, null]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product Name",
					ecommerceCategory1: "Category",
					ecommercePrice:     19.99,
					ecommerceQuantity:  1.0,
				},
			},
			expectError: false,
			description: "Defaults missing quantity to 1",
		},
		{
			name: "DefaultMissingCategory",
			raw:  `[["SKU123", "Product Name", null, 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product Name",
					ecommerceCategory1: "",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Defaults missing category to empty string",
		},
		{
			name: "CategoryStringOnly",
			raw:  `[["SKU123", "Product Name", "SingleCategory", 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product Name",
					ecommerceCategory1: "SingleCategory",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "String category maps to category_1 only",
		},
		{
			name: "CategoryArrayMultipleSlots",
			raw:  `[["SKU123", "Product Name", ["Cat1", "Cat2", "Cat3", "Cat4", "Cat5"], 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product Name",
					ecommerceCategory1: "Cat1",
					ecommerceCategory2: "Cat2",
					ecommerceCategory3: "Cat3",
					ecommerceCategory4: "Cat4",
					ecommerceCategory5: "Cat5",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Array category maps to all category slots",
		},
		{
			name: "CategoryArrayWithOverflow",
			raw:  `[["SKU123", "Product Name", ["Cat1", "Cat2", "Cat3", "Cat4", "Cat5", "Extra"], 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product Name",
					ecommerceCategory1: "Cat1",
					ecommerceCategory2: "Cat2",
					ecommerceCategory3: "Cat3",
					ecommerceCategory4: "Cat4",
					ecommerceCategory5: "Cat5",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Array category ignores values after slot 5",
		},
		{
			name:        "InvalidJSON",
			raw:         `invalid json`,
			expected:    nil,
			expectError: true,
			description: "Invalid JSON returns error",
		},
		{
			name:        "EmptyString",
			raw:         ``,
			expected:    nil,
			expectError: false,
			description: "Empty input returns nil",
		},
		{
			name:        "EmptyJSONArray",
			raw:         `[]`,
			expected:    []map[string]any{},
			expectError: false,
			description: "Empty JSON array returns empty non-nil slice",
		},
		{
			name: "SkipsNonArrayItems",
			raw:  `["not array", ["SKU123", "Product", "Category", 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product",
					ecommerceCategory1: "Category",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Skips non-array items",
		},
		{
			name: "SkipsMissingSkuSlot",
			raw:  `[["SKU123", "Product", "Category", 19.99, 2], [null, "NoSKU", "Category", 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "SKU123",
					ecommerceName:      "Product",
					ecommerceCategory1: "Category",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Skips items with missing/empty SKU",
		},
		{
			name: "NumericSkuCoercion",
			raw:  `[[123.45, "Product", "Category", 19.99, 2]]`,
			expected: []map[string]any{
				{
					ecommerceSKU:       "123.45",
					ecommerceName:      "Product",
					ecommerceCategory1: "Category",
					ecommercePrice:     19.99,
					ecommerceQuantity:  2.0,
				},
			},
			expectError: false,
			description: "Coerces numeric SKU to string",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result, err := parseEcommerceItems(tc.raw)

			// then
			if tc.expectError {
				assert.Error(t, err, tc.description)
			} else {
				assert.NoError(t, err, tc.description)
				assert.Equal(t, tc.expected, result, tc.description)
			}
		})
	}
}

func TestParseEcommerceItemCategory(t *testing.T) {
	type testCase struct {
		name        string
		raw         any
		expected    [5]any
		description string
	}

	testCases := []testCase{
		{
			name:        "StringCategory",
			raw:         "SingleCategory",
			expected:    [5]any{"SingleCategory", nil, nil, nil, nil},
			description: "String category populates only category_1",
		},
		{
			name:        "ArrayCategory",
			raw:         []any{"Cat1", "Cat2", "Cat3", "Cat4", "Cat5"},
			expected:    [5]any{"Cat1", "Cat2", "Cat3", "Cat4", "Cat5"},
			description: "Array category populates all five slots",
		},
		{
			name:        "ArrayWithOverflow",
			raw:         []any{"Cat1", "Cat2", "Cat3", "Cat4", "Cat5", "Extra", "More"},
			expected:    [5]any{"Cat1", "Cat2", "Cat3", "Cat4", "Cat5"},
			description: "Array category ignores values after slot 5",
		},
		{
			name:        "EmptyString",
			raw:         "",
			expected:    [5]any{nil, nil, nil, nil, nil},
			description: "Empty string leaves all slots nil",
		},
		{
			name:        "ArrayWithNulls",
			raw:         []any{"Cat1", nil, "Cat3"},
			expected:    [5]any{"Cat1", nil, "Cat3", nil, nil},
			description: "Array with null/empty values passes through",
		},
		{
			name:        "EmptyArray",
			raw:         []any{},
			expected:    [5]any{nil, nil, nil, nil, nil},
			description: "Empty array leaves all slots nil",
		},
		{
			name:        "MissingCategory",
			raw:         nil,
			expected:    [5]any{nil, nil, nil, nil, nil},
			description: "Missing/nil category leaves all slots nil",
		},
		{
			name:        "StringSlice",
			raw:         []string{"Cat1", "Cat2", "Cat3"},
			expected:    [5]any{"Cat1", "Cat2", "Cat3", nil, nil},
			description: "String slice category populates correct slots",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := parseEcommerceItemCategory(tc.raw)

			// then
			assert.Equal(t, tc.expected, result, tc.description)
		})
	}
}

func TestEventEcommerceItemsTotalQuantityColumn_SkipsMalformedRows(t *testing.T) {
	testCases := []struct {
		name     string
		items    any
		expected int64
	}{
		{
			name: "SkipsMalformedRows",
			items: []any{
				"not-a-map",
				map[string]any{ecommerceQuantity: "2"},
				map[string]any{ecommerceQuantity: 1.0},
				123,
				map[string]any{ecommerceQuantity: 4.0},
			},
			expected: int64(5),
		},
		{
			name:     "NonSliceItemsReturnsZero",
			items:    "unexpected",
			expected: int64(0),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &schema.Event{Values: map[string]any{ProtocolInterfaces.EventEcommerceItems.Field.Name: tc.items}}

			require.NotPanics(t, func() {
				err := eventEcommerceItemsTotalQuantityColumn.Write(event)
				require.NoError(t, err)
			})

			assert.Equal(t, tc.expected, event.Values[ProtocolInterfaces.EventEcommerceItemsTotalQuantity.Field.Name])
		})
	}
}
