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
	EventMeasurementID schema.Interface

	EventIParamgnoreReferrer   schema.Interface
	EventParamEngagementTimeMs schema.Interface

	// E-commerce columns
	EventEcommercePurchaseRevenue      schema.Interface
	EventEcommercePurchaseRevenueInUSD schema.Interface
	EventEcommerceRefundValue          schema.Interface
	EventEcommerceRefundValueInUSD     schema.Interface
	EventEcommerceShippingValue        schema.Interface
	EventEcommerceShippingValueInUSD   schema.Interface
	EventEcommerceTaxValue             schema.Interface
	EventEcommerceTaxValueInUSD        schema.Interface
	EventEcommerceItemsTotalQuantity   schema.Interface
	EventEcommerceUniqueItems          schema.Interface
	EventItems                         schema.Interface

	// Session columns
	GaSessionID                  schema.Interface
	GaSessionNumber              schema.Interface
	EventGtmDebug                schema.Interface
	EventPageLoadHash            schema.Interface
	EventParams                  schema.Interface
	EventParamAclid              schema.Interface
	EventPrivacyAdsStorage       schema.Interface
	EventPrivacyAnalyticsStorage schema.Interface
	EventParamAnid               schema.Interface

	// Event params
	EventParamAchievementID               schema.Interface
	EventParamAdEventID                   schema.Interface
	EventParamAdUnitCode                  schema.Interface
	EventParamAppVersion                  schema.Interface
	EventParamCampaign                    schema.Interface
	EventParamCampaignContent             schema.Interface
	EventParamCampaignID                  schema.Interface
	EventParamCampaignMedium              schema.Interface
	EventParamCampaignSource              schema.Interface
	EventParamCampaignTerm                schema.Interface
	EventParamCancellationReason          schema.Interface
	EventParamCharacter                   schema.Interface
	EventParamContentDescription          schema.Interface
	EventParamContentGroup                schema.Interface
	EventParamContentID                   schema.Interface
	EventParamContentType                 schema.Interface
	EventParamCoupon                      schema.Interface
	EventParamCreativeName                schema.Interface
	EventParamCreativeSlot                schema.Interface
	EventParamCurrency                    schema.Interface
	EventParamDclid                       schema.Interface
	EventParamDeferredAnalyticsCollection schema.Interface
	EventParamDisqualifiedLeadReason      schema.Interface
	EventParamExposureTime                schema.Interface
	EventParamFatal                       schema.Interface
	EventParamFileExtension               schema.Interface
	EventParamFileName                    schema.Interface
	EventParamFirebaseError               schema.Interface
	EventParamFirebaseErrorValue          schema.Interface
	EventParamFirebasePreviousClass       schema.Interface
	EventParamFirebasePreviousID          schema.Interface
	EventParamFirebasePreviousScreen      schema.Interface
	EventParamFirebaseScreen              schema.Interface
	EventParamFirebaseScreenClass         schema.Interface
	EventParamFirebaseScreenID            schema.Interface
	EventParamFormDestination             schema.Interface
	EventParamFormID                      schema.Interface
	EventParamFormName                    schema.Interface
	EventParamFormSubmitText              schema.Interface
	EventParamFreeTrial                   schema.Interface
	EventParamGaSessionID                 schema.Interface
	EventParamGaSessionNumber             schema.Interface
	EventParamGclid                       schema.Interface
	EventParamGroupID                     schema.Interface
	EventParamIntroductoryPrice           schema.Interface
	EventParamItemListID                  schema.Interface
	EventParamItemListName                schema.Interface
	EventParamItemName                    schema.Interface
	EventParamLabel                       schema.Interface
	EventParamLanguage                    schema.Interface
	EventParamLeadSource                  schema.Interface
	EventParamLeadStatus                  schema.Interface
	EventParamLevel                       schema.Interface
	EventParamLevelName                   schema.Interface
	EventParamLinkClasses                 schema.Interface
	EventParamLinkDomain                  schema.Interface
	EventParamLinkID                      schema.Interface
	EventParamLinkText                    schema.Interface
	EventParamLinkURL                     schema.Interface
	EventParamMessageDeviceTime           schema.Interface
	EventParamMessageID                   schema.Interface
	EventParamMessageName                 schema.Interface
	EventParamMessageTime                 schema.Interface
	EventParamMessageType                 schema.Interface
	EventParamMethod                      schema.Interface
	EventParamOutbound                    schema.Interface
	EventParamPaymentType                 schema.Interface
	EventParamPercentScrolled             schema.Interface
	EventParamPreviousAppVersion          schema.Interface
	EventParamPreviousFirstOpenCount      schema.Interface
	EventParamPreviousGmpAppID            schema.Interface
	EventParamPreviousOSVersion           schema.Interface
	EventParamPrice                       schema.Interface
	EventParamProductID                   schema.Interface
	EventParamPromotionID                 schema.Interface
	EventParamPromotionName               schema.Interface
	EventParamQuantity                    schema.Interface
	EventParamRenewalCount                schema.Interface
	EventParamResetAnalyticsCause         schema.Interface
	EventParamRewardType                  schema.Interface
	EventParamRewardValue                 schema.Interface
	EventParamScore                       schema.Interface
	EventParamScreenResolution            schema.Interface
	EventParamSearchTerm                  schema.Interface
	EventParamShipping                    schema.Interface
	EventParamShippingTier                schema.Interface
	EventParamSrsltid                     schema.Interface
	EventParamSubscription                schema.Interface
	EventParamSuccess                     schema.Interface
	EventParamSystemApp                   schema.Interface
	EventParamSystemAppUpdate             schema.Interface
	EventParamTax                         schema.Interface
	EventParamTopic                       schema.Interface
	EventParamTransactionID               schema.Interface
	EventParamUnconvertLeadReason         schema.Interface
	EventParamUpdatedWithAnalytics        schema.Interface
	EventParamValue                       schema.Interface
	EventParamVideoCurrentTime            schema.Interface
	EventParamVideoDuration               schema.Interface
	EventParamVideoPercent                schema.Interface
	EventParamVideoProvider               schema.Interface
	EventParamVideoTitle                  schema.Interface
	EventParamVideoURL                    schema.Interface
	EventParamVirtualCurrencyName         schema.Interface
	EventParamVisible                     schema.Interface

	// Page navigation columns
	EventNextPageLocation     schema.Interface
	EventPreviousPageLocation schema.Interface
	EventNextPageTitle        schema.Interface
	EventPreviousPageTitle    schema.Interface

	// Session engagement
	SessionIsEngaged     schema.Interface
	SessionReturningUser schema.Interface
	SessionAbandonedCart schema.Interface

	// Item params
	EventParamItemProductID schema.Interface
	EventParamItemPrice     schema.Interface
	EventParamItemQuantity  schema.Interface

	// Source columns extracted from page URL
	EventSourceManualCampaignID      schema.Interface
	EventSourceManualCampaignName    schema.Interface
	EventSourceManualSource          schema.Interface
	EventSourceManualMedium          schema.Interface
	EventSourceManualTerm            schema.Interface
	EventSourceManualContent         schema.Interface
	EventSourceManualSourcePlatform  schema.Interface
	EventSourceManualCreativeFormat  schema.Interface
	EventSourceManualMarketingTactic schema.Interface
	EventSourceGclid                 schema.Interface
	EventSourceDclid                 schema.Interface
	EventSourceSrsltid               schema.Interface

	EventGl schema.Interface
}{
	EventMeasurementID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/measurement_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "measurement_id", Type: arrow.BinaryTypes.String},
	},
	// ignore_referrer - used in session_start event
	EventIParamgnoreReferrer: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ignore_referrer",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ignore_referrer", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	// engagement_time_msec - used in user_engagement event (automatically collected)
	EventParamEngagementTimeMs: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_engagement_time_ms",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_engagement_time_ms", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventEcommercePurchaseRevenue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_purchase_revenue",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_purchase_revenue", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommercePurchaseRevenueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_purchase_revenue_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_purchase_revenue_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceRefundValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_refund_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_refund_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceRefundValueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_refund_value_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_refund_value_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceShippingValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_shipping_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_shipping_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceShippingValueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_shipping_value_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_shipping_value_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceTaxValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_tax_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_tax_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceTaxValueInUSD: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_tax_value_in_usd",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_tax_value_in_usd", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceItemsTotalQuantity: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_items_total_quantity",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_items_total_quantity", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventEcommerceUniqueItems: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ecommerce_unique_items",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ecommerce_unique_items", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventItems: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/items",
		Version: "1.0.0",
		Field: &arrow.Field{
			Nullable: true,
			Name:     "ecommerce_items", Type: arrow.ListOf(arrow.StructOf(
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
	GaSessionID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ga_session_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ga_session_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	GaSessionNumber: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/ga_session_number",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "ga_session_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventGtmDebug: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/gtm_debug",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "gtm_debug", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPageLoadHash: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/page_load_hash",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "page_load_hash", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	},
	EventParams: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params",
		Version: "1.0.0",
		Field: &arrow.Field{
			Nullable: true,
			Name:     "params", Type: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value_string", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value_number", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			)),
		},
	},
	EventParamAclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_aclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_aclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPrivacyAdsStorage: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/privacy_ads_storage",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "privacy_ads_storage", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventPrivacyAnalyticsStorage: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/privacy_analytics_storage",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "privacy_analytics_storage", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamAnid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_anid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_anid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamAchievementID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_achievement_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_achievement_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamAdEventID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ad_event_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ad_event_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamAdUnitCode: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ad_unit_code",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ad_unit_code", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamAppVersion: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_app_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_app_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCampaign: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCampaignContent: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_content",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_content", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCampaignID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCampaignMedium: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_medium",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_medium", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCampaignSource: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_source",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCampaignTerm: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_campaign_term",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_campaign_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCancellationReason: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_cancellation_reason",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_cancellation_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCharacter: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_character",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_character", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamContentDescription: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_description",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_description", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamContentGroup: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_group",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_group", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamContentID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamContentType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_content_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_content_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCoupon: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_coupon",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_coupon", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCreativeName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_creative_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_creative_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCreativeSlot: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_creative_slot",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_creative_slot", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamCurrency: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_currency",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_currency", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamDclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_dclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_dclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamDeferredAnalyticsCollection: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_deferred_analytics_collection",
		Version: "1.0.0",
		Field: &arrow.Field{
			Name:     "params_deferred_analytics_collection",
			Type:     arrow.FixedWidthTypes.Boolean,
			Nullable: true,
		},
	},
	EventParamDisqualifiedLeadReason: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_disqualified_lead_reason",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_disqualified_lead_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamExposureTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_exposure_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_exposure_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamFatal: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_fatal",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_fatal", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamFileExtension: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_file_extension",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_file_extension", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFileName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_file_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_file_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebaseError: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_error",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_error", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebaseErrorValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_error_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_error_value", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebasePreviousClass: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_previous_class",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_previous_class", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebasePreviousID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_previous_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_previous_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebasePreviousScreen: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_previous_screen",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_previous_screen", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebaseScreen: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_screen",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_screen", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebaseScreenClass: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_screen_class",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_screen_class", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFirebaseScreenID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_firebase_screen_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_firebase_screen_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFormDestination: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_destination",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_destination", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFormID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFormName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFormSubmitText: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_form_submit_text",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_form_submit_text", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamFreeTrial: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_free_trial",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_free_trial", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamGaSessionID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ga_session_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ga_session_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamGaSessionNumber: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_ga_session_number",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_ga_session_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamGclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_gclid",
		Version: "1.0.0",
		Field: &arrow.Field{
			Name:     "params_gclid",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
	},
	EventParamGroupID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_group_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_group_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamIntroductoryPrice: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_introductory_price",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_introductory_price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamItemListID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_item_list_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_item_list_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamItemListName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_item_list_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_item_list_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamItemName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_item_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_item_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLabel: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_label",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_label", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLanguage: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_language",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_language", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLeadSource: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_lead_source",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_lead_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLeadStatus: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_lead_status",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_lead_status", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLevel: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_level",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_level", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamLevelName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_level_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_level_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLinkClasses: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_classes",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_classes", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLinkDomain: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_domain",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_domain", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLinkID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLinkText: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_text",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_text", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamLinkURL: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_link_url",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_link_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamMessageDeviceTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_device_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_device_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamMessageID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamMessageName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamMessageTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamMessageType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_message_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_message_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamMethod: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_method",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_method", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamOutbound: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_outbound",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_outbound", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamPaymentType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_payment_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_payment_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamPercentScrolled: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_percent_scrolled",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_percent_scrolled", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamPreviousAppVersion: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_app_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_app_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamPreviousFirstOpenCount: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_first_open_count",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_first_open_count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamPreviousGmpAppID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_gmp_app_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_gmp_app_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamPreviousOSVersion: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_previous_os_version",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_previous_os_version", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamPrice: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_price",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamProductID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_product_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_product_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamPromotionID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_promotion_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_promotion_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamPromotionName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_promotion_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_promotion_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamQuantity: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_quantity",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_quantity", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamRenewalCount: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_renewal_count",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_renewal_count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamResetAnalyticsCause: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_reset_analytics_cause",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_reset_analytics_cause", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamRewardType: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_reward_type",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_reward_type", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamRewardValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_reward_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_reward_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamScore: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_score",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamScreenResolution: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_screen_resolution",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_screen_resolution", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamSearchTerm: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_search_term",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_search_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamShipping: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_shipping",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_shipping", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamShippingTier: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_shipping_tier",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_shipping_tier", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamSrsltid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/events/params_srsltid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_srsltid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamSubscription: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_subscription",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_subscription", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamSuccess: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_success",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_success", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamSystemApp: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_system_app",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_system_app", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamSystemAppUpdate: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_system_app_update",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_system_app_update", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamTax: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_tax",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamTopic: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_topic",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_topic", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamTransactionID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_transaction_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_transaction_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamUnconvertLeadReason: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_unconvert_lead_reason",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_unconvert_lead_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamUpdatedWithAnalytics: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_updated_with_analytics",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_updated_with_analytics", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventParamValue: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_value",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamVideoCurrentTime: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_current_time",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_current_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamVideoDuration: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_duration",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_duration", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamVideoPercent: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_percent",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_percent", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamVideoProvider: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_provider",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_provider", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamVideoTitle: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_title",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamVideoURL: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_video_url",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_video_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamVirtualCurrencyName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_virtual_currency_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_virtual_currency_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamVisible: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/params_visible",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_visible", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	},
	EventNextPageLocation: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/next_page_location",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "next_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousPageLocation: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/previous_page_location",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "previous_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventNextPageTitle: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/next_page_title",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "next_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousPageTitle: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/previous_page_title",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "previous_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	SessionIsEngaged: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/session/is_engaged",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_is_engaged", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionReturningUser: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/session/returning_user",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_returning_user", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionAbandonedCart: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/session/abandoned_cart",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "session_abandoned_cart", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventParamItemProductID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/item/params_product_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_product_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamItemPrice: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/item/params_price",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamItemQuantity: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/item/params_quantity",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "params_quantity", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	EventSourceManualCampaignID: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_campaign_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_campaign_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualCampaignName: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_campaign_name",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_campaign_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualSource: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_source",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_source", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualMedium: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_medium",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_medium", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualTerm: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_term",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualContent: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_content",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_content", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualSourcePlatform: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_source_platform",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_source_platform", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualCreativeFormat: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_creative_format",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_creative_format", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceManualMarketingTactic: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_manual_marketing_tactic",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_manual_marketing_tactic", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceGclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_gclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_gclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceDclid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_dclid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_dclid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSourceSrsltid: schema.Interface{
		ID:      "ga4.protocols.d8a.tech/event/source_srsltid",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "source_srsltid", Type: arrow.BinaryTypes.String, Nullable: true},
	},
}
