package ga4

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
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

			items, ok := record["items"].([]any)
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
			assert.Equal(t, 3.0, item["quantity"])
			assert.Equal(t, "Summer Creative", item["creative_name"])
			assert.Equal(t, "header-banner", item["creative_slot"])
			assert.Equal(t, "PROMO_123", item["promotion_id"])
			assert.Equal(t, "Summer Sale", item["promotion_name"])
		},
		NewGA4Protocol(),
		columntests.EnsureQueryParam(
			0,
			"pr1",
			"idSKU_12345~nmStan and Friends Tee~afGoogle Store~cpSUMMER_FUN~ds2.22~lp5~brGoogle~caApparel~c2Adult~c3Shirts~c4Crew~c5Short sleeve~lirelated_products~lnRelated products~vagreen~loChIJIQBpAG2ahYAR_6128GcTUEo~pr10.01~qt3~cnSummer Creative~csheader-banner~piPROMO_123~pnSummer Sale", // nolint:lll // contains all item params
		),
	)
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

			items, ok := record["items"].([]any)
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
		NewGA4Protocol(),
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
