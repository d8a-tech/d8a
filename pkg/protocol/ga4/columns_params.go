package ga4

import (
	"slices"
	"strings"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var genericEventParamsColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventParams.ID,
	ProtocolInterfaces.EventParams.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		params := make([]any, 0)
		for qpName, qpValues := range event.BoundHit.MustParsedRequest().QueryParams {
			if name, ok := strings.CutPrefix(qpName, "ep."); ok {
				for _, qpValue := range qpValues {
					params = append(params, map[string]any{
						"name":         name,
						"value_string": qpValue,
						"value_number": nil,
					})
				}
			} else if name, ok := strings.CutPrefix(qpName, "epn."); ok {
				for _, qpValue := range qpValues {
					numValue, err := columns.CastToFloat64OrNil(ProtocolInterfaces.EventParams.ID)(qpValue)
					if err != nil || numValue == nil {
						continue
					}
					params = append(params, map[string]any{
						"name":         name,
						"value_string": nil,
						"value_number": numValue,
					})
				}
			}
		}
		slices.SortFunc(params, func(a, b any) int {
			aMap, ok := a.(map[string]any)
			if !ok {
				return 0
			}
			bMap, ok := b.(map[string]any)
			if !ok {
				return 0
			}
			aName, _ := aMap["name"].(string)
			bName, _ := bMap["name"].(string)
			return strings.Compare(aName, bName)
		})
		return params, nil
	},
	columns.WithEventColumnDocs(
		"Event Params",
		"All the parameters associated with the event.",
	),
)

var eventContentGroupColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamContentGroup.ID,
	ProtocolInterfaces.EventParamContentGroup.Field,
	"ep.content_group",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamContentGroup.ID)),
	),
	columns.WithEventColumnDocs(
		"Content Group",
		"A category grouping for content (e.g., 'blog', 'videos', 'products').",
	),
)

var eventContentIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamContentID.ID,
	ProtocolInterfaces.EventParamContentID.Field,
	"ep.content_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamContentID.ID)),
	),
	columns.WithEventColumnDocs(
		"Content ID",
		"The unique identifier for a piece of content (e.g., 'article_001', 'video_xyz').",
	),
)

var eventContentTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamContentType.ID,
	ProtocolInterfaces.EventParamContentType.Field,
	"ep.content_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamContentType.ID)),
	),
	columns.WithEventColumnDocs(
		"Content Type",
		"The type of content viewed or interacted with (e.g., 'article', 'video', 'product').",
	),
)

var eventContentDescriptionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamContentDescription.ID,
	ProtocolInterfaces.EventParamContentDescription.Field,
	"ep.content",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamContentDescription.ID)),
	),
	columns.WithEventColumnDocs(
		"Content Description",
		"A description of the content viewed or interacted with (e.g., 'product review', 'how-to guide').",
	),
)

var eventCampaignColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaign.ID,
	ProtocolInterfaces.EventParamCampaign.Field,
	"ep.campaign",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaign.ID)),
	),
	columns.WithEventColumnDocs(
		"Campaign",
		"The campaign name associated with the traffic source (e.g., 'summer_sale', 'brand_awareness').",
	),
)

var eventCampaignIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaignID.ID,
	ProtocolInterfaces.EventParamCampaignID.Field,
	"ep.campaign_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaignID.ID)),
	),
	columns.WithEventColumnDocs(
		"Campaign ID",
		"The unique identifier for the campaign (e.g., 'camp_12345', 'Q1_2024_001').",
	),
)

var eventCampaignSourceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaignSource.ID,
	ProtocolInterfaces.EventParamCampaignSource.Field,
	"ep.campaign_source",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaignSource.ID)),
	),
	columns.WithEventColumnDocs(
		"Campaign Source",
		"The source of the campaign traffic (e.g., 'google', 'newsletter', 'facebook').",
	),
)

var eventCampaignMediumColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaignMedium.ID,
	ProtocolInterfaces.EventParamCampaignMedium.Field,
	"ep.campaign_medium",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaignMedium.ID)),
	),
	columns.WithEventColumnDocs(
		"Campaign Medium",
		"The medium of the campaign traffic (e.g., 'cpc', 'email', 'social').",
	),
)

var eventCampaignContentColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaignContent.ID,
	ProtocolInterfaces.EventParamCampaignContent.Field,
	"ep.campaign_content",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaignContent.ID)),
	),
	columns.WithEventColumnDocs(
		"Campaign Content",
		"Used to differentiate ads or links within the same campaign (e.g., 'banner_blue', 'textlink_red').",
	),
)

var eventCampaignTermColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaignTerm.ID,
	ProtocolInterfaces.EventParamCampaignTerm.Field,
	"ep.campaign_term",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaignTerm.ID)),
	),
	columns.WithEventColumnDocs(
		"Campaign Term",
		"The paid search keywords for the campaign (e.g., 'running shoes', 'best laptop').",
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventCouponColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCoupon.ID,
	ProtocolInterfaces.EventParamCoupon.Field,
	"ep.coupon",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCoupon.ID)),
	),
	columns.WithEventColumnDocs(
		"Coupon",
		"The coupon code applied to a transaction (e.g., 'SAVE20', 'FREESHIP').",
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventCurrencyColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCurrency.ID,
	ProtocolInterfaces.EventParamCurrency.Field,
	"ep.currency",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCurrency.ID)),
	),
	columns.WithEventColumnDocs(
		"Currency",
		"The currency code for monetary values (e.g., 'USD', 'EUR', 'GBP').",
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventShippingColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamShipping.ID,
	ProtocolInterfaces.EventParamShipping.Field,
	"epn.shipping",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamShipping.ID)),
	columns.WithEventColumnDocs(
		"Shipping",
		"The shipping cost for a transaction.",
	),
)

// On surface duplicates the above - nevertheless it's in the dataform, so including it for now
// I guess the reasoning that is the former contains raw param value, while the latter
// draws some conclusions, like using zero value if the param is empty
var eventEcommerceShippingValueColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventEcommerceShippingValue.ID,
	ProtocolInterfaces.EventEcommerceShippingValue.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		eventName := event.Values[columns.CoreInterfaces.EventName.Field.Name]
		if eventName != PurchaseEventType && eventName != RefundEventType {
			return float64(0), nil
		}
		shipping := event.Values[ProtocolInterfaces.EventParamShipping.Field.Name]
		if shipping == nil {
			return float64(0), nil
		}
		shippingAsFloat, ok := shipping.(float64)
		if !ok {
			return float64(0), nil
		}
		return shippingAsFloat, nil
	},
	columns.WithEventColumnDocs(
		"Ecommerce Shipping Value",
		"The shipping cost associated with the transaction, extracted from the params_shipping parameter, with zero as default if not present. Only populated for purchase and refund events.", // nolint:lll // it's a description
	),
	columns.WithEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: ProtocolInterfaces.EventParamShipping.ID,
		},
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventShippingTierColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamShippingTier.ID,
	ProtocolInterfaces.EventParamShippingTier.Field,
	"ep.shipping_tier",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamShippingTier.ID)),
	),
	columns.WithEventColumnDocs(
		"Shipping Tier",
		"The shipping tier or method selected (e.g., 'standard', 'express', 'overnight').",
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventPaymentTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPaymentType.ID,
	ProtocolInterfaces.EventParamPaymentType.Field,
	"ep.payment_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPaymentType.ID)),
	),
	columns.WithEventColumnDocs(
		"Payment Type",
		"The payment method used for a transaction (e.g., 'credit_card', 'paypal', 'apple_pay').",
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventParamTaxColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamTax.ID,
	ProtocolInterfaces.EventParamTax.Field,
	"epn.tax",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamTax.ID)),
	columns.WithEventColumnDocs(
		"Tax",
		"The tax amount for a transaction.",
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventTransactionIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamTransactionID.ID,
	ProtocolInterfaces.EventParamTransactionID.Field,
	"ep.transaction_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamTransactionID.ID)),
	),
	columns.WithEventColumnDocs(
		"Transaction ID",
		"The unique identifier for a transaction (e.g., 'T12345', 'order_abc123').",
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamValue.ID,
	ProtocolInterfaces.EventParamValue.Field,
	"epn.value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamValue.ID)),
	columns.WithEventColumnDocs(
		"Value",
		"The monetary value associated with an event.",
	),
)

var eventItemListIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamItemListID.ID,
	ProtocolInterfaces.EventParamItemListID.Field,
	"ep.item_list_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamItemListID.ID)),
	),
	columns.WithEventColumnDocs(
		"Item List ID",
		"The identifier for a list of items (e.g., 'related_products', 'search_results').",
	),
)

var eventItemListNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamItemListName.ID,
	ProtocolInterfaces.EventParamItemListName.Field,
	"ep.item_list_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamItemListName.ID)),
	),
	columns.WithEventColumnDocs(
		"Item List Name",
		"The name of a list of items (e.g., 'Related Products', 'Search Results').",
	),
)

var eventCreativeNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCreativeName.ID,
	ProtocolInterfaces.EventParamCreativeName.Field,
	"ep.creative_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCreativeName.ID)),
	),
	columns.WithEventColumnDocs(
		"Creative Name",
		"The name of the creative used in advertising (e.g., 'summer_banner_v2', 'product_showcase').",
	),
)

var eventCreativeSlotColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCreativeSlot.ID,
	ProtocolInterfaces.EventParamCreativeSlot.Field,
	"ep.creative_slot",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCreativeSlot.ID)),
	),
	columns.WithEventColumnDocs(
		"Creative Slot",
		"The position or slot where the creative was displayed (e.g., 'slot_1', 'homepage_hero').",
	),
)

var eventPromotionIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPromotionID.ID,
	ProtocolInterfaces.EventParamPromotionID.Field,
	"ep.promotion_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPromotionID.ID)),
	),
	columns.WithEventColumnDocs(
		"Promotion ID",
		"The unique identifier for a promotion (e.g., 'promo_001', 'summer_sale_2024').",
	),
)

var eventPromotionNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPromotionName.ID,
	ProtocolInterfaces.EventParamPromotionName.Field,
	"ep.promotion_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPromotionName.ID)),
	),
	columns.WithEventColumnDocs(
		"Promotion Name",
		"The name of a promotion (e.g., 'Summer Sale', 'BOGO Deal').",
	),
)

// Ad related params (ad_exposure, ad_query, ad_impression, ad_reward)
var eventAdEventIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAdEventID.ID,
	ProtocolInterfaces.EventParamAdEventID.Field,
	"ep.ad_event_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAdEventID.ID)),
	),
	columns.WithEventColumnDocs(
		"Ad Event ID",
		"A unique identifier for an ad event (e.g., 'ad_click_001', 'impression_xyz').",
	),
)

var eventExposureTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamExposureTime.ID,
	ProtocolInterfaces.EventParamExposureTime.Field,
	"ep.exposure_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamExposureTime.ID)),
	columns.WithEventColumnDocs(
		"Exposure Time",
		"The time a promotion or element was visible to the user.",
	),
)

var eventAdUnitCodeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAdUnitCode.ID,
	ProtocolInterfaces.EventParamAdUnitCode.Field,
	"ep.ad_unit_code",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAdUnitCode.ID)),
	),
	columns.WithEventColumnDocs(
		"Ad Unit Code",
		"The code or name of the ad unit where an ad is displayed (e.g., 'banner_top', 'sidebar_300x250').",
	),
)

var eventRewardTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamRewardType.ID,
	ProtocolInterfaces.EventParamRewardType.Field,
	"ep.reward_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamRewardType.ID)),
	),
	columns.WithEventColumnDocs(
		"Reward Type",
		"The type of reward earned (e.g., 'coins', 'badge', 'power_up').",
	),
)

var eventRewardValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamRewardValue.ID,
	ProtocolInterfaces.EventParamRewardValue.Field,
	"epn.reward_value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamRewardValue.ID)),
	columns.WithEventColumnDocs(
		"Reward Value",
		"The value or amount of the reward earned.",
	),
)

// Video params
var eventVideoCurrentTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoCurrentTime.ID,
	ProtocolInterfaces.EventParamVideoCurrentTime.Field,
	"epn.video_current_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamVideoCurrentTime.ID)),
	columns.WithEventColumnDocs(
		"Video Current Time",
		"The current playback time of a video in seconds.",
	),
)

var eventVideoDurationColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoDuration.ID,
	ProtocolInterfaces.EventParamVideoDuration.Field,
	"epn.video_duration",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamVideoDuration.ID)),
	columns.WithEventColumnDocs(
		"Video Duration",
		"The total duration of a video in seconds.",
	),
)

var eventVideoPercentColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoPercent.ID,
	ProtocolInterfaces.EventParamVideoPercent.Field,
	"ep.video_percent",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamVideoPercent.ID)),
	columns.WithEventColumnDocs(
		"Video Percent",
		"The percentage of a video watched.",
	),
)

var eventVideoProviderColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoProvider.ID,
	ProtocolInterfaces.EventParamVideoProvider.Field,
	"ep.video_provider",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamVideoProvider.ID)),
	),
	columns.WithEventColumnDocs(
		"Video Provider",
		"The provider or platform hosting the video (e.g., 'youtube', 'vimeo', 'self-hosted').",
	),
)

var eventVideoTitleColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoTitle.ID,
	ProtocolInterfaces.EventParamVideoTitle.Field,
	"ep.video_title",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamVideoTitle.ID)),
	),
	columns.WithEventColumnDocs(
		"Video Title",
		"The title of the video (e.g., 'Product Demo', 'How-To Tutorial').",
	),
)

var eventVideoURLColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoURL.ID,
	ProtocolInterfaces.EventParamVideoURL.Field,
	"ep.video_url",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamVideoURL.ID)),
	),
	columns.WithEventColumnDocs(
		"Video URL",
		"The URL of the video (e.g., 'https://youtube.com/watch?v=abc').",
	),
)

// EventLink columns for outbound click tracking
var eventLinkClassesColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLinkClasses.ID,
	ProtocolInterfaces.EventParamLinkClasses.Field,
	"ep.link_classes",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLinkClasses.ID)),
	),
	columns.WithEventColumnDocs(
		"Link Classes",
		"The CSS classes of a clicked link (e.g., 'btn btn-primary', 'nav-link').",
	),
)

var eventLinkDomainColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLinkDomain.ID,
	ProtocolInterfaces.EventParamLinkDomain.Field,
	"ep.link_domain",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLinkDomain.ID)),
	),
	columns.WithEventColumnDocs(
		"Link Domain",
		"The domain of a clicked link (e.g., 'example.com', 'partner-site.org').",
	),
)

var eventLinkIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLinkID.ID,
	ProtocolInterfaces.EventParamLinkID.Field,
	"ep.link_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLinkID.ID)),
	),
	columns.WithEventColumnDocs(
		"Link ID",
		"The ID attribute of a clicked link (e.g., 'cta_button', 'footer_link_1').",
	),
)

var eventLinkTextColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLinkText.ID,
	ProtocolInterfaces.EventParamLinkText.Field,
	"ep.link_text",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLinkText.ID)),
	),
	columns.WithEventColumnDocs(
		"Link Text",
		"The visible text of a clicked link (e.g., 'Learn More', 'Download Now').",
	),
)

var eventLinkURLColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLinkURL.ID,
	ProtocolInterfaces.EventParamLinkURL.Field,
	"ep.link_url",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLinkURL.ID)),
	),
	columns.WithEventColumnDocs(
		"Link URL",
		"The full URL of a clicked link (e.g., 'https://example.com/page').",
	),
)

var eventOutboundColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamOutbound.ID,
	ProtocolInterfaces.EventParamOutbound.Field,
	"ep.outbound",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamOutbound.ID))),
	columns.WithEventColumnDocs(
		"Outbound",
		"Indicates if a link click is outbound.",
	),
)

// App params
var eventMethodColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMethod.ID,
	ProtocolInterfaces.EventParamMethod.Field,
	"ep.method",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamMethod.ID)),
	),
	columns.WithEventColumnDocs(
		"Method",
		"The method used for an action like signup, login or share events (e.g., 'Google', 'Facebook', 'Email').",
	),
)

var eventCancellationReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCancellationReason.ID,
	ProtocolInterfaces.EventParamCancellationReason.Field,
	"ep.cancellation_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCancellationReason.ID)),
	),
	columns.WithEventColumnDocs(
		"Cancellation Reason",
		"The reason a user canceled a subscription or service (e.g., 'too_expensive', 'not_using_enough').",
	),
)

var eventFatalColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFatal.ID,
	ProtocolInterfaces.EventParamFatal.Field,
	"ep.fatal",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamFatal.ID))),
	columns.WithEventColumnDocs(
		"Fatal",
		"Indicates if an error or exception was fatal.",
	),
)

// Firebase params
var eventFirebaseErrorColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebaseError.ID,
	ProtocolInterfaces.EventParamFirebaseError.Field,
	"ep.firebase_error",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebaseError.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Error",
		"The type or code of a Firebase error (e.g., 'auth_failed', 'network_timeout').",
	),
)

var eventFirebaseErrorValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebaseErrorValue.ID,
	ProtocolInterfaces.EventParamFirebaseErrorValue.Field,
	"ep.firebase_error_value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebaseErrorValue.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Error Value",
		"The specific value or message of a Firebase error (e.g., 'invalid_credentials', 'connection_lost').",
	),
)

var eventFirebaseScreenColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebaseScreen.ID,
	ProtocolInterfaces.EventParamFirebaseScreen.Field,
	"ep.firebase_screen",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebaseScreen.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Screen",
		"The name of the current screen viewed in the app (e.g., 'Home', 'Checkout').",
	),
)

var eventFirebaseScreenClassColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebaseScreenClass.ID,
	ProtocolInterfaces.EventParamFirebaseScreenClass.Field,
	"ep.firebase_screen_class",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebaseScreenClass.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Screen Class",
		"The class name of the current screen in the app (e.g., 'MainActivity', 'CheckoutActivity').",
	),
)

var eventFirebaseScreenIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebaseScreenID.ID,
	ProtocolInterfaces.EventParamFirebaseScreenID.Field,
	"ep.firebase_screen_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebaseScreenID.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Screen ID",
		"The identifier of the current screen in the app (e.g., 'screen_home', 'screen_checkout').",
	),
)

var eventFirebasePreviousScreenColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebasePreviousScreen.ID,
	ProtocolInterfaces.EventParamFirebasePreviousScreen.Field,
	"ep.firebase_previous_screen",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebasePreviousScreen.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Previous Screen",
		"The name of the previous screen viewed in the app (e.g., 'Home', 'Product Detail').",
	),
)

var eventFirebasePreviousClassColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebasePreviousClass.ID,
	ProtocolInterfaces.EventParamFirebasePreviousClass.Field,
	"ep.firebase_previous_class",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebasePreviousClass.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Previous Class",
		"The class name of the previous screen in the app (e.g., 'MainActivity', 'ProfileActivity').",
	),
)

var eventFirebasePreviousIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebasePreviousID.ID,
	ProtocolInterfaces.EventParamFirebasePreviousID.Field,
	"ep.firebase_previous_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebasePreviousID.ID)),
	),
	columns.WithEventColumnDocs(
		"Firebase Previous ID",
		"The identifier of the previous screen in the app (e.g., 'screen_home', 'screen_profile').",
	),
)

// Message params - used in firebase_in_app_message_(action|dismiss|impression), fiam_(action|dismiss|impression)
// notification_(foreground|open|dismiss|receive)
var eventMessageDeviceTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageDeviceTime.ID,
	ProtocolInterfaces.EventParamMessageDeviceTime.Field,
	"ep.message_device_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamMessageDeviceTime.ID)),
	columns.WithEventColumnDocs(
		"Message Device Time",
		"The device time when a message was sent or received.",
	),
)

var eventMessageIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageID.ID,
	ProtocolInterfaces.EventParamMessageID.Field,
	"ep.message_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamMessageID.ID)),
	),
	columns.WithEventColumnDocs(
		"Message ID",
		"The unique identifier for a message (e.g., 'msg_12345', 'notification_abc').",
	),
)

var eventMessageNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageName.ID,
	ProtocolInterfaces.EventParamMessageName.Field,
	"ep.message_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamMessageName.ID)),
	),
	columns.WithEventColumnDocs(
		"Message Name",
		"The name or title of a message or notification (e.g., 'Welcome Email', 'Promotion Alert').",
	),
)

var eventMessageTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageTime.ID,
	ProtocolInterfaces.EventParamMessageTime.Field,
	"ep.message_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamMessageTime.ID)),
	columns.WithEventColumnDocs(
		"Message Time",
		"The server time when a message was sent or received.",
	),
)

var eventMessageTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageType.ID,
	ProtocolInterfaces.EventParamMessageType.Field,
	"ep.message_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamMessageType.ID)),
	),
	columns.WithEventColumnDocs(
		"Message Type",
		"The type of message sent or received (e.g., 'notification', 'in-app_message', 'email').",
	),
)

var eventTopicColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamTopic.ID,
	ProtocolInterfaces.EventParamTopic.Field,
	"ep.topic",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamTopic.ID)),
	),
	columns.WithEventColumnDocs(
		"Topic",
		"The topic or category of content (e.g., 'technology', 'sports', 'finance').",
	),
)

var eventLabelColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLabel.ID,
	ProtocolInterfaces.EventParamLabel.Field,
	"ep.label",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLabel.ID)),
	),
	columns.WithEventColumnDocs(
		"Label",
		"A custom label for categorization or tracking (e.g., 'promo_click', 'special_offer').",
	),
)

// App params - used in app events
var eventAppVersionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAppVersion.ID,
	ProtocolInterfaces.EventParamAppVersion.Field,
	"ep.app_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAppVersion.ID)),
	),
	columns.WithEventColumnDocs(
		"App Version",
		"The version number of the application (e.g., '1.2.3', '2.0.1').",
	),
)

var eventPreviousAppVersionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousAppVersion.ID,
	ProtocolInterfaces.EventParamPreviousAppVersion.Field,
	"ep.previous_app_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPreviousAppVersion.ID)),
	),
	columns.WithEventColumnDocs(
		"Previous App Version",
		"The previous version of the app before an update (e.g., '1.1.0', '2.3.5').",
	),
)

var eventPreviousFirstOpenCountColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousFirstOpenCount.ID,
	ProtocolInterfaces.EventParamPreviousFirstOpenCount.Field,
	"ep.previous_first_open_count",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamPreviousFirstOpenCount.ID)),
	columns.WithEventColumnDocs(
		"Previous First Open Count",
		"The count of first opens before the current session.",
	),
)

var eventPreviousOSVersionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousOSVersion.ID,
	ProtocolInterfaces.EventParamPreviousOSVersion.Field,
	"ep.previous_os_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPreviousOSVersion.ID)),
	),
	columns.WithEventColumnDocs(
		"Previous OS Version",
		"The previous operating system version before an update (e.g., 'iOS 14.0', 'Android 10').",
	),
)

var eventUpdatedWithAnalyticsColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamUpdatedWithAnalytics.ID,
	ProtocolInterfaces.EventParamUpdatedWithAnalytics.Field,
	"ep.updated_with_analytics",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamUpdatedWithAnalytics.ID)),
	),
	columns.WithEventColumnDocs(
		"Updated with Analytics",
		"Indicates if analytics was updated.",
	),
)

// Gaming params - used in gaming events
var eventAchievementIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAchievementID.ID,
	ProtocolInterfaces.EventParamAchievementID.Field,
	"ep.achievement_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAchievementID.ID)),
	),
	columns.WithEventColumnDocs(
		"Achievement ID",
		"The ID of the achievement unlocked (e.g., 'A_12345', 'first_win').",
	),
)

var eventCharacterColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCharacter.ID,
	ProtocolInterfaces.EventParamCharacter.Field,
	"ep.character",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCharacter.ID)),
	),
	columns.WithEventColumnDocs(
		"Character",
		"The character selected or used by the player (e.g., 'warrior', 'Player 1').",
	),
)

var eventLevelColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLevel.ID,
	ProtocolInterfaces.EventParamLevel.Field,
	"ep.level",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamLevel.ID)),
	columns.WithEventColumnDocs(
		"Level",
		"The level number in a game or progression system.",
	),
)

var eventLevelNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLevelName.ID,
	ProtocolInterfaces.EventParamLevelName.Field,
	"ep.level_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLevelName.ID)),
	),
	columns.WithEventColumnDocs(
		"Level Name",
		"The name of a level in a game or progression system (e.g., 'Tutorial Island', 'Boss Battle 3').",
	),
)

var eventScoreColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamScore.ID,
	ProtocolInterfaces.EventParamScore.Field,
	"epn.score",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamScore.ID)),
	columns.WithEventColumnDocs(
		"Score",
		"The score achieved in a game or activity.",
	),
)

var eventVirtualCurrencyNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVirtualCurrencyName.ID,
	ProtocolInterfaces.EventParamVirtualCurrencyName.Field,
	"ep.virtual_currency_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamVirtualCurrencyName.ID)),
	),
	columns.WithEventColumnDocs(
		"Virtual Currency Name",
		"The name of the virtual currency used (e.g., 'coins', 'gems', 'credits').",
	),
)

var eventItemNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamItemName.ID,
	ProtocolInterfaces.EventParamItemName.Field,
	"ep.item_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamItemName.ID)),
	),
	columns.WithEventColumnDocs(
		"Item Name",
		"The name of an item or product (e.g., 'Blue Running Shoes', 'Wireless Headphones').",
	),
)

var eventSuccessColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSuccess.ID,
	ProtocolInterfaces.EventParamSuccess.Field,
	"ep.success",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSuccess.ID))),
	columns.WithEventColumnDocs(
		"Success",
		"Indicates if an action was successful.",
	),
)

// System params - automatically collected with app events
var eventVisibleColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVisible.ID,
	ProtocolInterfaces.EventParamVisible.Field,
	"ep.visible",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamVisible.ID))),
	columns.WithEventColumnDocs(
		"Visible",
		"Indicates if an element was visible to the user.",
	),
)

var eventScreenResolutionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamScreenResolution.ID,
	ProtocolInterfaces.EventParamScreenResolution.Field,
	"ep.screen_resolution",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamScreenResolution.ID)),
	),
	columns.WithEventColumnDocs(
		"Screen Resolution",
		"The screen resolution of the user's device (e.g., '1920x1080', '375x667').",
	),
)

var eventSystemAppColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSystemApp.ID,
	ProtocolInterfaces.EventParamSystemApp.Field,
	"ep.system_app",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSystemApp.ID))),
	columns.WithEventColumnDocs(
		"System App",
		"Indicates if an app is a system app.",
	),
)

var eventSystemAppUpdateColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSystemAppUpdate.ID,
	ProtocolInterfaces.EventParamSystemAppUpdate.Field,
	"ep.system_app_update",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSystemAppUpdate.ID))),
	columns.WithEventColumnDocs(
		"System App Update",
		"Indicates if a system app was updated.",
	),
)

var eventDeferredAnalyticsCollectionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamDeferredAnalyticsCollection.ID,
	ProtocolInterfaces.EventParamDeferredAnalyticsCollection.Field,
	"ep.deferred_analytics_collection",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamDeferredAnalyticsCollection.ID)),
	),
	columns.WithEventColumnDocs(
		"Deferred Analytics Collection",
		"Indicates if analytics collection was deferred.", // nolint:lll // it's a description
	),
)

var eventResetAnalyticsCauseColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamResetAnalyticsCause.ID,
	ProtocolInterfaces.EventParamResetAnalyticsCause.Field,
	"ep.reset_analytics_cause",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamResetAnalyticsCause.ID)),
	),
	columns.WithEventColumnDocs(
		"Reset Analytics Cause",
		"The reason analytics data was reset (e.g., 'user_request', 'app_reinstall').",
	),
)

var eventPreviousGmpAppIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousGmpAppID.ID,
	ProtocolInterfaces.EventParamPreviousGmpAppID.Field,
	"ep.previous_gmp_app_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPreviousGmpAppID.ID)),
	),
	columns.WithEventColumnDocs(
		"Previous GMP App ID",
		"The previous Google Mobile Platform app identifier (e.g., 'app_id_123', 'old_app_456').",
	),
)

// Form and file params - used in form and file events
var eventFileExtensionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFileExtension.ID,
	ProtocolInterfaces.EventParamFileExtension.Field,
	"ep.file_extension",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFileExtension.ID)),
	),
	columns.WithEventColumnDocs(
		"File Extension",
		"The extension of a downloaded or interacted file (e.g., 'pdf', 'jpg', 'xlsx').",
	),
)

var eventFileNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFileName.ID,
	ProtocolInterfaces.EventParamFileName.Field,
	"ep.file_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFileName.ID)),
	),
	columns.WithEventColumnDocs(
		"File Name",
		"The name of a downloaded or interacted file (e.g., 'whitepaper.pdf', 'product_catalog.xlsx').",
	),
)

var eventFormDestinationColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFormDestination.ID,
	ProtocolInterfaces.EventParamFormDestination.Field,
	"ep.form_destination",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFormDestination.ID)),
	),
	columns.WithEventColumnDocs(
		"Form Destination",
		"The destination URL or page after form submission (e.g., '/thank-you', '/confirmation').",
	),
)

var eventFormIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFormID.ID,
	ProtocolInterfaces.EventParamFormID.Field,
	"ep.form_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFormID.ID)),
	),
	columns.WithEventColumnDocs(
		"Form ID",
		"The unique identifier of the form (e.g., 'contact_form_01', 'newsletter_signup').",
	),
)

var eventFormNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFormName.ID,
	ProtocolInterfaces.EventParamFormName.Field,
	"ep.form_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFormName.ID)),
	),
	columns.WithEventColumnDocs(
		"Form Name",
		"The name of the form (e.g., 'Contact Us', 'Newsletter Subscription').",
	),
)

var eventFormSubmitTextColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFormSubmitText.ID,
	ProtocolInterfaces.EventParamFormSubmitText.Field,
	"ep.form_submit_text",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFormSubmitText.ID)),
	),
	columns.WithEventColumnDocs(
		"Form Submit Text",
		"The text on the form submit button (e.g., 'Submit', 'Send Message', 'Subscribe').",
	),
)

// Engagement params

var eventGroupIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamGroupID.ID,
	ProtocolInterfaces.EventParamGroupID.Field,
	"ep.group_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamGroupID.ID)),
	),
	columns.WithEventColumnDocs(
		"Group ID",
		"The identifier for a group or cohort (e.g., 'group_a', 'beta_testers').",
	),
)

var eventLanguageColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLanguage.ID,
	ProtocolInterfaces.EventParamLanguage.Field,
	"ep.language",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLanguage.ID)),
	),
	columns.WithEventColumnDocs(
		"Language",
		"The language code of the content or interface (e.g., 'en', 'es', 'fr').",
	),
)

var eventPercentScrolledColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPercentScrolled.ID,
	ProtocolInterfaces.EventParamPercentScrolled.Field,
	"epn.percent_scrolled",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamPercentScrolled.ID)),
	columns.WithEventColumnDocs(
		"Percent Scrolled",
		"The percentage of a page scrolled by the user.",
	),
)

var eventSearchTermColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSearchTerm.ID,
	ProtocolInterfaces.EventParamSearchTerm.Field,
	"ep.search_term",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamSearchTerm.ID)),
	),
	columns.WithEventColumnDocs(
		"Search Term",
		"The search query entered by the user (e.g., 'running shoes', 'best laptop 2024').",
	),
)

// Lead params

var eventUnconvertLeadReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamUnconvertLeadReason.ID,
	ProtocolInterfaces.EventParamUnconvertLeadReason.Field,
	"ep.unconvert_lead_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamUnconvertLeadReason.ID)),
	),
	columns.WithEventColumnDocs(
		"Unconvert Lead Reason",
		"The reason a lead was unconverted or lost (e.g., 'no_response', 'chose_competitor').",
	),
)

var eventDisqualifiedLeadReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamDisqualifiedLeadReason.ID,
	ProtocolInterfaces.EventParamDisqualifiedLeadReason.Field,
	"ep.disqualified_lead_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamDisqualifiedLeadReason.ID)),
	),
	columns.WithEventColumnDocs(
		"Disqualified Lead Reason",
		"The reason a lead was disqualified (e.g., 'out_of_territory', 'invalid_contact').",
	),
)

var eventLeadSourceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLeadSource.ID,
	ProtocolInterfaces.EventParamLeadSource.Field,
	"ep.lead_source",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLeadSource.ID)),
	),
	columns.WithEventColumnDocs(
		"Lead Source",
		"The source from which a lead originated (e.g., 'website_form', 'trade_show', 'referral').",
	),
)

var eventLeadStatusColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLeadStatus.ID,
	ProtocolInterfaces.EventParamLeadStatus.Field,
	"ep.lead_status",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLeadStatus.ID)),
	),
	columns.WithEventColumnDocs(
		"Lead Status",
		"The status of a lead in the sales process (e.g., 'qualified', 'contacted', 'converted').",
	),
)

var eventFreeTrialColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFreeTrial.ID,
	ProtocolInterfaces.EventParamFreeTrial.Field,
	"ep.free_trial",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamFreeTrial.ID))),
	columns.WithEventColumnDocs(
		"Free Trial",
		"Indicates if a subscription or in app purchase includes a free trial.",
	),
)

var eventSubscriptionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSubscription.ID,
	ProtocolInterfaces.EventParamSubscription.Field,
	"ep.subscription",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSubscription.ID))),
	columns.WithEventColumnDocs(
		"Subscription",
		"Indicates if a purchase is a subscription.",
	),
)

var eventProductIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamProductID.ID,
	ProtocolInterfaces.EventParamProductID.Field,
	"ep.product_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamProductID.ID)),
	),
	columns.WithEventColumnDocs(
		"Product ID",
		"The unique identifier for a product (e.g., 'SKU12345', 'prod_abc').",
	),
)

var eventPriceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPrice.ID,
	ProtocolInterfaces.EventParamPrice.Field,
	"epn.price",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamPrice.ID),
	),
	columns.WithEventColumnDocs(
		"Price",
		"The price of an item or product.",
	),
)

var eventQuantityColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamQuantity.ID,
	ProtocolInterfaces.EventParamQuantity.Field,
	"epn.quantity",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamQuantity.ID),
	),
	columns.WithEventColumnDocs(
		"Quantity",
		"The quantity of items in a transaction or action.",
	),
)

var eventIntroductoryPriceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamIntroductoryPrice.ID,
	ProtocolInterfaces.EventParamIntroductoryPrice.Field,
	"epn.introductory_price",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamIntroductoryPrice.ID),
	),
	columns.WithEventColumnDocs(
		"Introductory Price",
		"The introductory or promotional price for a product or subscription.",
	),
)

var gclidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamGclid.ID,
	ProtocolInterfaces.EventParamGclid.Field,
	"ep.gclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamGclid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google gclid (Param)",
		"The Google Click Identifier for attribution from Google Ads (e.g., 'gclid_abc123def').",
	),
)

var dclidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamDclid.ID,
	ProtocolInterfaces.EventParamDclid.Field,
	"ep.dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamDclid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google dclid (Param)",
		"The DoubleClick Click Identifier for attribution (e.g., 'dclid_xyz789').",
	),
)

var srsltidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSrsltid.ID,
	ProtocolInterfaces.EventParamSrsltid.Field,
	"ep.srsltid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamSrsltid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google srsltid (Param)",
		"The Shopping Ads result ID for attribution (e.g., 'srsltid_12345').",
	),
)

var aclidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAclid.ID,
	ProtocolInterfaces.EventParamAclid.Field,
	"ep.aclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAclid.ID)),
	),
	columns.WithEventColumnDocs(
		"Apple aclid (Param)",
		"The Apple Search Ads campaign ID for attribution (e.g., 'aclid_12345').",
	),
)

var anidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAnid.ID,
	ProtocolInterfaces.EventParamAnid.Field,
	"ep.anid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAnid.ID)),
	),
	columns.WithEventColumnDocs(
		"Android anid (Param)",
		"The Android Advertising ID for attribution (e.g., 'anid_abc123').",
	),
)

var gaSessionIDParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamGaSessionID.ID,
	ProtocolInterfaces.EventParamGaSessionID.Field,
	"ep.ga_session_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamGaSessionID.ID)),
	),
	columns.WithEventColumnDocs(
		"GA Session ID",
		"The Google Analytics 4 session identifier. A unique identifier for the current session, used to group events into sessions. Extracted from the first-party cookie. Use only to compare numbers with GA4. For real session data calculated on the backend, use the session_id column.", // nolint:lll // it's a description
	),
)

var gaSessionNumberParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamGaSessionNumber.ID,
	ProtocolInterfaces.EventParamGaSessionNumber.Field,
	"epn.ga_session_number",
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamGaSessionNumber.ID)),
	columns.WithEventColumnDocs(
		"GA Session Number",
		"The Google Analytics 4 sequential count of sessions for this user. Increments with each new session (e.g., 1 for first session, 2 for second). Extracted from the first-party cookie.", // nolint:lll // it's a description
	),
)

var renewalCountParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamRenewalCount.ID,
	ProtocolInterfaces.EventParamRenewalCount.Field,
	"epn.renewal_count",
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamRenewalCount.ID)),
	columns.WithEventColumnDocs(
		"Renewal Count",
		"The number of times a subscription has been renewed.",
	),
)
