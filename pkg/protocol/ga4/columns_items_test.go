package ga4

import (
	"fmt"
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestItemsColumnAllParams(t *testing.T) {
	// given
	columntests.ColumnTestCase(
		t,
		columntests.TestHits{columntests.TestHitOne()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			record := whd.WriteCalls[0].Records[0]

			items, ok := record["ecommerce_items"].([]any)
			require.True(t, ok, "items should be of type []any")
			assert.Len(t, items, 1)

			item, ok := items[0].(map[string]any)
			require.True(t, ok, "item should be of type map[string]any")
			assert.Equal(t, "SKU_12345", item["item_id"])
			assert.Equal(t, "Stan and Friends Tee", item["item_name"])
			assert.Equal(t, "Google Store", item["affiliation"])
			assert.Equal(t, "SUMMER_FUN", item["coupon"])
			assert.Equal(t, 2.22, item["discount"])
			assert.Equal(t, 5.0, item["index"])
			assert.Equal(t, "Google", item["item_brand"])
			assert.Equal(t, "Apparel", item["item_category"])
			assert.Equal(t, "Adult", item["item_category2"])
			assert.Equal(t, "Shirts", item["item_category3"])
			assert.Equal(t, "Crew", item["item_category4"])
			assert.Equal(t, "Short sleeve", item["item_category5"])
			assert.Equal(t, "related_products", item["item_list_id"])
			assert.Equal(t, "Related products", item["item_list_name"])
			assert.Equal(t, "green", item["item_variant"])
			assert.Equal(t, "ChIJIQBpAG2ahYAR_6128GcTUEo", item["location_id"])
			assert.Equal(t, 10.01, item["price"])
			assert.Equal(t, 20.02, item["price_in_usd"])
			assert.Equal(t, float64(0), item["item_refund"])
			assert.Equal(t, float64(0), item["item_refund_in_usd"])
			assert.Equal(t, 30.03, item["item_revenue"])
			assert.Equal(t, 60.06, item["item_revenue_in_usd"])
			assert.Equal(t, 3.0, item["quantity"])
			assert.Equal(t, "Summer Creative", item["creative_name"])
			assert.Equal(t, "header-banner", item["creative_slot"])
			assert.Equal(t, "PROMO_123", item["promotion_id"])
			assert.Equal(t, "Summer Sale", item["promotion_name"])
		},
		NewGA4Protocol(currency.NewDummyConverter(2), properties.NewTestSettingRegistry()),
		columntests.EnsureQueryParam(
			0,
			"pr1",
			"idSKU_12345~nmStan and Friends Tee~afGoogle Store~cpSUMMER_FUN~ds2.22~lp5~brGoogle~caApparel~c2Adult~c3Shirts~c4Crew~c5Short sleeve~lirelated_products~lnRelated products~vagreen~loChIJIQBpAG2ahYAR_6128GcTUEo~pr10.01~qt3~cnSummer Creative~csheader-banner~piPROMO_123~pnSummer Sale", // nolint:lll // contains all item params
		),
		columntests.EnsureQueryParam(
			0,
			"ep.currency",
			"EUR",
		),
	)
}

func TestItemsColumnRefund(t *testing.T) {
	// given
	testCases := []struct {
		name            string
		itemID          string
		price           float64
		quantity        float64
		eventName       string
		expectedRefund  float64
		expectedRevenue float64
	}{
		{
			name:            "basic refund",
			itemID:          "SKU_12345",
			price:           10.01,
			quantity:        3.0,
			eventName:       "refund",
			expectedRefund:  30.03,
			expectedRevenue: 0.0,
		},
		{
			name:            "basic revenue",
			itemID:          "SKU_12345",
			price:           10.01,
			quantity:        3.0,
			eventName:       "purchase",
			expectedRefund:  0.0,
			expectedRevenue: 30.03,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				columntests.TestHits{columntests.TestHitOne()},
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					record := whd.WriteCalls[0].Records[0]

					items, ok := record["ecommerce_items"].([]any)
					require.True(t, ok, "items should be of type []any")
					assert.Len(t, items, 1)

					item, ok := items[0].(map[string]any)
					require.True(t, ok, "item should be of type map[string]any")
					assert.Equal(t, tc.itemID, item["item_id"])
					assert.Equal(t, tc.price, item["price"])
					assert.Equal(t, tc.quantity, item["quantity"])
					assert.Equal(t, tc.expectedRefund, item["item_refund"])
					assert.Equal(t, tc.expectedRevenue, item["item_revenue"])
				},
				NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				columntests.EnsureQueryParam(
					0,
					"pr1",
					"id"+tc.itemID+"~pr"+fmt.Sprintf("%.2f", tc.price)+"~qt"+fmt.Sprintf("%.0f", tc.quantity),
				),
				columntests.EnsureEventName(
					0,
					tc.eventName,
				),
			)
		})
	}
}

func TestItemsColumnTakeFromEvent(t *testing.T) {
	// given
	columntests.ColumnTestCase(
		t,
		columntests.TestHits{columntests.TestHitOne()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			record := whd.WriteCalls[0].Records[0]

			items, ok := record["ecommerce_items"].([]any)
			require.True(t, ok, "items should be of type []any")
			assert.Len(t, items, 1)

			item, ok := items[0].(map[string]any)
			require.True(t, ok, "item should be of type map[string]any")
			assert.Equal(t, "SKU_12345", item["item_id"])
			assert.Equal(t, "related_products", item["item_list_id"])
			assert.Equal(t, "Related products", item["item_list_name"])
			assert.Equal(t, "Event Creative", item["creative_name"])
			assert.Equal(t, "event-banner", item["creative_slot"])
			assert.Equal(t, "EVENT_PROMO_456", item["promotion_id"])
			assert.Equal(t, "Event Promotion", item["promotion_name"])
		},
		NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
		columntests.EnsureQueryParam(
			0,
			"pr1",
			"idSKU_12345",
		),
		columntests.EnsureQueryParam(
			0,
			"ep.item_list_id",
			"related_products",
		),
		columntests.EnsureQueryParam(
			0,
			"ep.item_list_name",
			"Related products",
		),
		columntests.EnsureQueryParam(
			0,
			"ep.creative_name",
			"Event Creative",
		),
		columntests.EnsureQueryParam(
			0,
			"ep.creative_slot",
			"event-banner",
		),
		columntests.EnsureQueryParam(
			0,
			"ep.promotion_id",
			"EVENT_PROMO_456",
		),
		columntests.EnsureQueryParam(
			0,
			"ep.promotion_name",
			"Event Promotion",
		),
	)
}

func TestItemsAggregatedEventParams(t *testing.T) {
	type item struct {
		itemID   string
		price    float64
		quantity float64
	}
	// given
	testCases := []struct {
		name         string
		baseCurrency string
		items        []item
		eventName    string
		tax          *float64
		shipping     *float64
		assertFunc   func(t *testing.T, record map[string]any)
	}{
		{
			name:         "basic refund",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.01, quantity: 3.0}},
			eventName:    "refund",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(3), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 30.03, record["ecommerce_refund_value"])
				assert.Equal(t, 15.02, record["ecommerce_refund_value_in_usd"])
			},
		},
		{
			name:         "basic revenue",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.01, quantity: 3.0}},
			eventName:    "purchase",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(3), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 30.03, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 15.02, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
			},
		},
		{
			name: "multiple items refund",
			items: []item{
				{itemID: "SKU_12345", price: 10.01, quantity: 3.0},
				{itemID: "SKU_12346", price: 10.02, quantity: 4.0},
			},
			eventName: "refund",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(2), record["ecommerce_unique_items"])
				assert.Equal(t, int64(7), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, nil, record["ecommerce_purchase_revenue_in_usd"]) // no currency defined
				assert.Equal(t, 70.11, record["ecommerce_refund_value"])
			},
		},
		{
			name: "multiple items revenue",
			items: []item{
				{itemID: "SKU_12345", price: 10.01, quantity: 3.0},
				{itemID: "SKU_12346", price: 10.02, quantity: 4.0},
			},
			eventName: "purchase",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(2), record["ecommerce_unique_items"])
				assert.Equal(t, int64(7), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 70.11, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
			},
		},
		{
			name: "multiple items same id",
			items: []item{
				{itemID: "SKU_12345", price: 10.01, quantity: 3.0},
				{itemID: "SKU_12345", price: 10.02, quantity: 4.0},
			},
			eventName: "purchase",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(7), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 70.11, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
			},
		},
		{
			name:         "refund value in USD conversion",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 20.0, quantity: 2.0}},
			eventName:    "refund",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(2), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 40.0, record["ecommerce_refund_value"])
				assert.Equal(t, 20.0, record["ecommerce_refund_value_in_usd"])
			},
		},
		{
			name:         "shipping value in USD conversion",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "purchase",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 10.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 5.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
				// shipping_value returns 0.0 when no ep.shipping param is provided
				assert.Equal(t, 0.0, record["ecommerce_shipping_value"])
				assert.Equal(t, 0.0, record["ecommerce_shipping_value_in_usd"])
			},
		},
		{
			name:         "shipping value in USD conversion with shipping param",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "purchase",
			shipping:     func() *float64 { v := 3.0; return &v }(),
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 10.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 5.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
				// shipping_value should be 3.0 EUR, shipping_value_in_usd should be 1.5 USD (3.0 * 0.5)
				assert.Equal(t, 3.0, record["ecommerce_shipping_value"])
				assert.Equal(t, 1.5, record["ecommerce_shipping_value_in_usd"])
			},
		},
		{
			name:         "tax value in USD conversion",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "purchase",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 10.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 5.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
				// tax_value and tax_value_in_usd will be nil without ep.tax param
				assert.Equal(t, nil, record["ecommerce_tax_value"])
				assert.Equal(t, nil, record["ecommerce_tax_value_in_usd"])
			},
		},
		{
			name:         "tax value in USD conversion with tax param",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "purchase",
			tax:          func() *float64 { v := 2.0; return &v }(),
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 10.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 5.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
				// params_tax should be 2.0 EUR, tax_value_in_usd should be 1.0 USD (2.0 * 0.5)
				assert.Equal(t, 2.0, record["ecommerce_tax_value"])
				assert.Equal(t, 1.0, record["ecommerce_tax_value_in_usd"])
			},
		},
		{
			name:         "purchase revenue zero for add_to_cart event",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 2.0}},
			eventName:    "add_to_cart",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(2), record["ecommerce_items_total_quantity"])
				// purchase_revenue should be 0 for non-purchase events
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
			},
		},
		{
			name:         "purchase revenue zero for view_item event",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "view_item",
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				// purchase_revenue should be 0 for non-purchase events
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
			},
		},
		{
			name:         "shipping value zero for add_to_cart event even with shipping param",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "add_to_cart",
			shipping:     func() *float64 { v := 5.0; return &v }(),
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				// shipping_value should be 0 for non-purchase/refund events
				assert.Equal(t, 0.0, record["ecommerce_shipping_value"])
				assert.Equal(t, 0.0, record["ecommerce_shipping_value_in_usd"])
			},
		},
		{
			name:         "shipping value populated for refund event",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "refund",
			shipping:     func() *float64 { v := 3.0; return &v }(),
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 0.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 10.0, record["ecommerce_refund_value"])
				// shipping_value should be populated for refund events
				assert.Equal(t, 3.0, record["ecommerce_shipping_value"])
				assert.Equal(t, 1.5, record["ecommerce_shipping_value_in_usd"])
			},
		},
		{
			name:         "shipping value populated for purchase event",
			baseCurrency: "EUR",
			items:        []item{{itemID: "SKU_12345", price: 10.0, quantity: 1.0}},
			eventName:    "purchase",
			shipping:     func() *float64 { v := 3.0; return &v }(),
			assertFunc: func(t *testing.T, record map[string]any) {
				assert.Equal(t, int64(1), record["ecommerce_unique_items"])
				assert.Equal(t, int64(1), record["ecommerce_items_total_quantity"])
				assert.Equal(t, 10.0, record["ecommerce_purchase_revenue"])
				assert.Equal(t, 5.0, record["ecommerce_purchase_revenue_in_usd"])
				assert.Equal(t, 0.0, record["ecommerce_refund_value"])
				// shipping_value should be populated for purchase events
				assert.Equal(t, 3.0, record["ecommerce_shipping_value"])
				assert.Equal(t, 1.5, record["ecommerce_shipping_value_in_usd"])
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given

			refFuncs := []columntests.CaseConfigFunc{
				columntests.EnsureEventName(
					0,
					tc.eventName,
				),
			}

			if tc.baseCurrency != "" {
				refFuncs = append(refFuncs, columntests.EnsureQueryParam(
					0,
					"ep.currency",
					tc.baseCurrency,
				))
			}

			if tc.tax != nil {
				refFuncs = append(refFuncs, columntests.EnsureQueryParam(
					0,
					"epn.tax",
					fmt.Sprintf("%.2f", *tc.tax),
				))
			}

			if tc.shipping != nil {
				refFuncs = append(refFuncs, columntests.EnsureQueryParam(
					0,
					"epn.shipping",
					fmt.Sprintf("%.2f", *tc.shipping),
				))
			}

			for n, item := range tc.items {
				refFuncs = append(refFuncs, columntests.EnsureQueryParam(
					0,
					fmt.Sprintf("pr%d", n+1),
					"id"+item.itemID+"~pr"+fmt.Sprintf("%.2f", item.price)+"~qt"+fmt.Sprintf("%.0f", item.quantity),
				))
			}
			columntests.ColumnTestCase(
				t,
				columntests.TestHits{columntests.TestHitOne()},
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					record := whd.WriteCalls[0].Records[0]

					tc.assertFunc(t, record)
				},
				NewGA4Protocol(currency.NewDummyConverter(.5), properties.NewTestSettingRegistry()),
				refFuncs...,
			)
		})
	}
}
