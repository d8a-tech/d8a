// Package ga4 provides GA4 protocol specific column definitions.
package ga4

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// Item attribute keys used inside the nested items array
const (
	itemKeyID            = "item_id"
	itemKeyName          = "item_name"
	itemKeyAffiliation   = "affiliation"
	itemKeyCoupon        = "coupon"
	itemKeyDiscount      = "discount"
	itemKeyIndex         = "index"
	itemKeyBrand         = "item_brand"
	itemKeyCategory      = "item_category"
	itemKeyCategory2     = "item_category2"
	itemKeyCategory3     = "item_category3"
	itemKeyCategory4     = "item_category4"
	itemKeyCategory5     = "item_category5"
	itemKeyListID        = "item_list_id"
	itemKeyListName      = "item_list_name"
	itemKeyVariant       = "item_variant"
	itemKeyLocationID    = "location_id"
	itemKeyPrice         = "price"
	itemKeyPriceInUSD    = "price_in_usd"
	itemKeyQuantity      = "quantity"
	itemKeyRefund        = "item_refund"
	itemKeyRefundInUSD   = "item_refund_in_usd"
	itemKeyRevenue       = "item_revenue"
	itemKeyRevenueInUSD  = "item_revenue_in_usd"
	itemKeyPromotionID   = "promotion_id"
	itemKeyPromotionName = "promotion_name"
	itemKeyCreativeName  = "creative_name"
	itemKeyCreativeSlot  = "creative_slot"
)

// ProtocolInterfaces are the columns that are specific to the ga4 protocol.
var ProtocolInterfaces = struct {
	EventIgnoreReferrer   schema.Interface
	EventEngagementTimeMs schema.Interface
	// Campaign params
	EventCampaign        schema.Interface
	EventCampaignID      schema.Interface
	EventCampaignSource  schema.Interface
	EventCampaignMedium  schema.Interface
	EventCampaignContent schema.Interface
	EventCampaignTerm    schema.Interface
	// Content params
	EventContentGroup       schema.Interface
	EventContentID          schema.Interface
	EventContentType        schema.Interface
	EventContentDescription schema.Interface
	// E-commerce params
	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
	EventCoupon               schema.Interface
	EventCurrency             schema.Interface
	EventShipping             schema.Interface
	EventShippingTier         schema.Interface
	EventPaymentType          schema.Interface
	EventTax                  schema.Interface
	EventTransactionID        schema.Interface
	EventValue                schema.Interface
	EventPurchaseRevenue      schema.Interface
	EventPurchaseRevenueInUSD schema.Interface
	EventRefundValue          schema.Interface
	EventRefundValueInUSD     schema.Interface
	EventShippingValue        schema.Interface
	EventShippingValueInUSD   schema.Interface
	EventTaxValueInUSD        schema.Interface
	EventUniqueItems          schema.Interface
	EventItemsTotalQuantity   schema.Interface

	// Item list params
	EventItemListID   schema.Interface
	EventItemListName schema.Interface
	// Creative and promotion params
	EventCreativeName  schema.Interface
	EventCreativeSlot  schema.Interface
	EventPromotionID   schema.Interface
	EventPromotionName schema.Interface
	// Link params
	EventLinkClasses schema.Interface
	EventLinkDomain  schema.Interface
	EventLinkID      schema.Interface
	EventLinkText    schema.Interface
	EventLinkURL     schema.Interface
	EventOutbound    schema.Interface
	// Ad related params (ad_exposure, ad_query, ad_impression, ad_reward)
	// https://support.google.com/analytics/answer/9234069?hl=en
	EventAdEventID    schema.Interface
	EventExposureTime schema.Interface
	EventAdUnitCode   schema.Interface
	EventRewardType   schema.Interface
	EventRewardValue  schema.Interface
	// Video params
	EventVideoCurrentTime schema.Interface
	EventVideoDuration    schema.Interface
	EventVideoPercent     schema.Interface
	EventVideoProvider    schema.Interface
	EventVideoTitle       schema.Interface
	EventVideoURL         schema.Interface
	// App params
	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
	EventMethod             schema.Interface // used in login, share, sign_up
	EventCancellationReason schema.Interface // used in app_store_subscription_cancel
	EventFatal              schema.Interface // used in app_exception

	// Firebase params
	EventFirebaseError          schema.Interface
	EventFirebaseErrorValue     schema.Interface
	EventFirebaseScreen         schema.Interface
	EventFirebaseScreenClass    schema.Interface
	EventFirebaseScreenID       schema.Interface
	EventFirebasePreviousScreen schema.Interface
	EventFirebasePreviousClass  schema.Interface
	EventFirebasePreviousID     schema.Interface
	// Subscription params
	EventFreeTrial         schema.Interface // used in in_app_purchase
	EventSubscription      schema.Interface // used in in_app_purchase
	EventProductID         schema.Interface // product_id
	EventPrice             schema.Interface // price
	EventQuantity          schema.Interface // quantity
	EventIntroductoryPrice schema.Interface // used in in_app_purchase
	EventRenewalCount      schema.Interface // used in app_store_subscription_renew
	// Message params
	// all used in firebase_in_app_message_(action|dismiss|impression), fiam_(action|dismiss|impression)
	// notification_(foreground|open|dismiss|receive)
	EventMessageDeviceTime schema.Interface
	EventMessageID         schema.Interface
	EventMessageName       schema.Interface

	// used in notification_(foreground|open|dismiss|receive)
	EventMessageTime schema.Interface
	EventMessageType schema.Interface
	EventTopic       schema.Interface
	EventLabel       schema.Interface

	// App params - automatically collected with app events
	// https://support.google.com/analytics/answer/9234069?hl=en
	EventAppVersion             schema.Interface // app_version - used in app events
	EventPreviousAppVersion     schema.Interface // previous_app_version - used in app events
	EventPreviousFirstOpenCount schema.Interface // previous_first_open_count - used in app events
	EventPreviousOSVersion      schema.Interface // previous_os_version - used in app events
	EventUpdatedWithAnalytics   schema.Interface // updated_with_analytics - used in app events
	// Gaming params - used in gaming events
	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag
	EventAchievementID       schema.Interface // achievement_id - used in unlock_achievement event
	EventCharacter           schema.Interface // character - used in level_up and other gaming events
	EventLevel               schema.Interface // level - used in level_up event
	EventLevelName           schema.Interface // level_name - used in level_up event
	EventScore               schema.Interface // score - used in gaming events
	EventVirtualCurrencyName schema.Interface // virtual_currency_name - earn_virtual_currency, spend_virtual_currency
	EventItemName            schema.Interface // item_name - used in spend_virtual_currency
	EventSuccess             schema.Interface // success - used in various gaming events
	// System params - automatically collected with app events
	// https://support.google.com/analytics/answer/9234069?hl=en
	EventVisible                     schema.Interface // visible - used in app events
	EventScreenResolution            schema.Interface // screen_resolution - used in app events
	EventSystemApp                   schema.Interface // system_app - used in app events
	EventSystemAppUpdate             schema.Interface // system_app_update - used in app events
	EventDeferredAnalyticsCollection schema.Interface // deferred_analytics_collection - used in app events
	EventResetAnalyticsCause         schema.Interface // reset_analytics_cause - used in app events
	EventPreviousGmpAppID            schema.Interface // previous_gmp_app_id - used in app events
	// Form and file params - used in form and file events
	EventFileExtension   schema.Interface // file_extension - used in file events
	EventFileName        schema.Interface // file_name - used in file events
	EventFormDestination schema.Interface // form_destination - used in form events
	EventFormID          schema.Interface // form_id - used in form events
	EventFormName        schema.Interface // form_name - used in form events
	EventFormSubmitText  schema.Interface // form_submit_text - used in form events

	// Engagement params
	// group_id - used in join_group event
	EventGroupID schema.Interface
	// language - automatically collected with web events (page_view, etc.)
	EventLanguage schema.Interface
	// percent_scrolled - used in scroll event (enhanced measurement)
	EventPercentScrolled schema.Interface
	// search_term - used in view_search_results event (enhanced measurement)
	EventSearchTerm schema.Interface
	// Lead params
	// unconvert_lead_reason - used in custom lead tracking events
	EventUnconvertLeadReason schema.Interface
	// disqualified_lead_reason - used in custom lead tracking events
	EventDisqualifiedLeadReason schema.Interface
	// lead_source - used in custom lead tracking events
	EventLeadSource schema.Interface
	// lead_status - used in custom lead tracking events
	EventLeadStatus schema.Interface
	// Session params
	SessionEngagement            schema.Interface
	SessionParamsGaSessionID     schema.Interface
	SessionParamsGaSessionNumber schema.Interface
	SessionNumber                schema.Interface

	// Item params
	EventItems    schema.Interface
	ItemName      schema.Interface
	ItemProductID schema.Interface
	ItemPrice     schema.Interface
	ItemQuantity  schema.Interface
	// Page URL params
	EventGtmDebug schema.Interface
	EventGl       schema.Interface

	// **lid params
	EventParamGclid   schema.Interface
	EventParamDclid   schema.Interface
	EventParamSrsltid schema.Interface
	EventParamAclid   schema.Interface
	EventParamAnid    schema.Interface
}{
	// ignore_referrer - used in session_start event
	EventIgnoreReferrer: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ignore_referrer",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ignore_referrer", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// engagement_time_msec - used in user_engagement event (automatically collected)
	EventEngagementTimeMs: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_engagement_time_ms",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_engagement_time_ms", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventContentGroup: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_group",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_group", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventContentID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventContentType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventContentDescription: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_description",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_description", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCampaign: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCampaignID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCampaignSource: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_source",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCampaignMedium: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_medium",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_medium", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCampaignContent: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_content",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_content", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCampaignTerm: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_term",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCoupon: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_coupon",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_coupon", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCurrency: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_currency",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_currency", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventShipping: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_shipping",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_shipping", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventShippingTier: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_shipping_tier",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_shipping_tier", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPaymentType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_payment_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_payment_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventTax: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_tax",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventTransactionID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_transaction_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_transaction_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventPurchaseRevenue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/purchase_revenue",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "purchase_revenue", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventPurchaseRevenueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/purchase_revenue_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "purchase_revenue_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventRefundValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/refund_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "refund_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventRefundValueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/refund_value_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "refund_value_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventShippingValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/shipping_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "shipping_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventShippingValueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/shipping_value_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "shipping_value_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventTaxValueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/tax_value_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "tax_value_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventUniqueItems: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/unique_items",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "unique_items", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventItemsTotalQuantity: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/items_total_quantity",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "items_total_quantity", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionEngagement: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/session/engagement",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "engagement", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionParamsGaSessionID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/session/params_ga_session_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ga_session_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionNumber: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/session/params_ga_session_number",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ga_session_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	// Item list params
	EventItemListID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_item_list_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_item_list_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventItemListName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_item_list_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_item_list_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Creative and promotion params
	EventCreativeName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_creative_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_creative_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCreativeSlot: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_creative_slot",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_creative_slot", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPromotionID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_promotion_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_promotion_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPromotionName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_promotion_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_promotion_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Item params
	ItemName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/item/params_item_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_item_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	ItemProductID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/item/params_product_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_product_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	ItemPrice: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/item/params_price",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	ItemQuantity: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/item/params_quantity",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_quantity", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventItems: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/items",
		Version: "1.0.0",
		Field: &arrow.Field{
			Nullable: true,
			Name:     "items", Type: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: itemKeyID, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyName, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyAffiliation, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCoupon, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyDiscount, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyIndex, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyBrand, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCategory, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCategory2, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCategory3, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCategory4, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCategory5, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyListID, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyListName, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyVariant, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyLocationID, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyPrice, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyPriceInUSD, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyQuantity, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyRefund, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyRefundInUSD, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyRevenue, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyRevenueInUSD, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: itemKeyPromotionID, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyPromotionName, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCreativeName, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: itemKeyCreativeSlot, Type: arrow.BinaryTypes.String, Nullable: true},
			)),
		},
	},
	// Link params
	EventLinkClasses: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_classes",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_classes", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLinkDomain: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_domain",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_domain", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLinkID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLinkText: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_text",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_text", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLinkURL: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_url",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventOutbound: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_outbound",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_outbound", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// Ad params
	EventAdUnitCode: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ad_unit_code",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ad_unit_code", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventAdEventID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ad_event_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ad_event_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventExposureTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_exposure_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_exposure_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	// Reward params
	EventRewardType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_reward_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_reward_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventRewardValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_reward_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_reward_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	// Video params
	EventVideoCurrentTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_current_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_current_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventVideoDuration: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_duration",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_duration", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventVideoPercent: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_percent",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_percent", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventVideoProvider: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_provider",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_provider", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventVideoTitle: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_title",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventVideoURL: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_url",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// App params
	EventMethod: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_method",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_method", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventAppVersion: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_app_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_app_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCancellationReason: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_cancellation_reason",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_cancellation_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFatal: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_fatal",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_fatal", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// Firebase params
	EventFirebaseError: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_error",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_error", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFirebaseErrorValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_error_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_error_value", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFirebaseScreen: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_screen",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_screen", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFirebaseScreenClass: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_screen_class",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_screen_class", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFirebaseScreenID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_screen_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_screen_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFirebasePreviousScreen: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_previous_screen",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_previous_screen", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFirebasePreviousClass: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_previous_class",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_previous_class", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFirebasePreviousID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_previous_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_previous_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Subscription params
	// free_trial - used in in_app_purchase event
	EventFreeTrial: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_free_trial",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_free_trial", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// subscription - used in in_app_purchase event
	EventSubscription: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_subscription",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_subscription", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// product_id - event level product ID
	EventProductID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_product_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_product_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// price - event level price
	EventPrice: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_price",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	// quantity - event level quantity
	EventQuantity: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_quantity",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_quantity", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	// introductory_price - used in in_app_purchase event
	EventIntroductoryPrice: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_introductory_price",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_introductory_price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	// renewal_count - used in app_store_subscription_renew event
	EventRenewalCount: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_renewal_count",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_renewal_count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	// Message params
	EventMessageDeviceTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_device_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_device_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventMessageID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventMessageName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventMessageTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventMessageType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventTopic: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_topic",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_topic", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Misc params
	EventLabel: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_label",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_label", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousAppVersion: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_app_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_app_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousFirstOpenCount: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_first_open_count",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_first_open_count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventPreviousOSVersion: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_os_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_os_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventUpdatedWithAnalytics: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_updated_with_analytics",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_updated_with_analytics", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// Gaming params
	EventAchievementID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_achievement_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_achievement_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventCharacter: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_character",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_character", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLevel: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_level",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_level", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventLevelName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_level_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_level_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventScore: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_score",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventVirtualCurrencyName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_virtual_currency_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_virtual_currency_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventItemName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_item_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_item_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSuccess: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_success",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_success", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// System params
	EventVisible: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_visible",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_visible", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventScreenResolution: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_screen_resolution",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_screen_resolution", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSystemApp: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_system_app",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_system_app", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventSystemAppUpdate: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_system_app_update",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_system_app_update", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventDeferredAnalyticsCollection: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_deferred_analytics_collection",
		Version: "1.0.0",
		Field: &arrow.Field{
			Name:     "params_deferred_analytics_collection",
			Type:     arrow.FixedWidthTypes.Boolean,
			Nullable: true,
		},
	},
	EventResetAnalyticsCause: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_reset_analytics_cause",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_reset_analytics_cause", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousGmpAppID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_gmp_app_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_gmp_app_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Form and file params
	EventFileExtension: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_file_extension",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_file_extension", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFileName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_file_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_file_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFormDestination: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_destination",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_destination", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFormID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFormName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventFormSubmitText: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_submit_text",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_submit_text", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Engagement params
	EventGroupID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_group_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_group_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLanguage: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_language",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_language", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPercentScrolled: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_percent_scrolled",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_percent_scrolled", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventSearchTerm: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_search_term",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_search_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Lead params
	EventUnconvertLeadReason: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_unconvert_lead_reason",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_unconvert_lead_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventDisqualifiedLeadReason: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_disqualified_lead_reason",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_disqualified_lead_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLeadSource: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_lead_source",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_lead_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventLeadStatus: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_lead_status",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_lead_status", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// Page URL params
	EventGtmDebug: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/gtm_debug",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "gtm_debug", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventGl: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/gl",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "_gl", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	// **lid params
	EventParamGclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_gclid",
		Version: "1.0.0",
		Field: &arrow.Field{
			Name:     "params_gclid",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
	},
	EventParamDclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_dclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_dclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamSrsltid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_srsltid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_srsltid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamAclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_aclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_aclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamAnid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_anid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_anid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
}
