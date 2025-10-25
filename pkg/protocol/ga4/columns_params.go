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
	func(event *schema.Event) (any, error) {
		params := make([]any, 0)
		for qpName, qpValues := range event.BoundHit.QueryParams {
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
		"Flattened GA4 event parameter from built-in/recommended events: content group.",
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
		"Flattened GA4 event parameter from built-in/recommended events: content identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: content type.",
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
		"Flattened GA4 event parameter from built-in/recommended events: content description.",
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
		"Flattened GA4 event parameter from built-in/recommended events: campaign name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: campaign identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: campaign source.",
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
		"Flattened GA4 event parameter from built-in/recommended events: campaign medium.",
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
		"Flattened GA4 event parameter from built-in/recommended events: campaign content.",
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
		"Flattened GA4 event parameter from built-in/recommended events: campaign term (keyword).",
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
		"Flattened GA4 event parameter from built-in/recommended events: coupon code.",
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
		"Flattened GA4 event parameter from built-in/recommended events: ISO currency code.",
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
		"Flattened GA4 event parameter from built-in/recommended events: shipping amount.",
	),
)

// On surface duplicates the above - nevertheless it's in the dataform, so including it for now
// I guess the reasoning that is the former contains raw param value, while the latter
// draws some conclusions, like using zero value if the param is empty
var eventEcommerceShippingValueColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventEcommerceShippingValue.ID,
	ProtocolInterfaces.EventEcommerceShippingValue.Field,
	func(event *schema.Event) (any, error) {
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
		"The shipping cost associated with the transaction, extracted from the params_shipping parameter, with zero as default if not present.", // nolint:lll // it's a description
	),
	columns.WithEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventParamShipping.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventParamShipping.Version,
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
		"Flattened GA4 event parameter from built-in/recommended events: shipping tier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: payment type.",
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
		"Flattened GA4 event parameter from built-in/recommended events: tax amount.",
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
		"Flattened GA4 event parameter from built-in/recommended events: transaction identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: numeric value associated with the event.",
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
		"Flattened GA4 event parameter from built-in/recommended events: item list ID.",
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
		"Flattened GA4 event parameter from built-in/recommended events: item list name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: creative name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: creative slot.",
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
		"Flattened GA4 event parameter from built-in/recommended events: promotion ID.",
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
		"Flattened GA4 event parameter from built-in/recommended events: promotion name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: ad event identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: ad exposure time in milliseconds.",
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
		"Flattened GA4 event parameter from built-in/recommended events: ad unit code.",
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
		"Flattened GA4 event parameter from built-in/recommended events: reward type.",
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
		"Flattened GA4 event parameter from built-in/recommended events: reward value.",
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
		"Flattened GA4 event parameter from built-in/recommended events: video current time in seconds.",
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
		"Flattened GA4 event parameter from built-in/recommended events: video duration in seconds.",
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
		"Flattened GA4 event parameter from built-in/recommended events: video completion percentage.",
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
		"Flattened GA4 event parameter from built-in/recommended events: video provider name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: video title.",
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
		"Flattened GA4 event parameter from built-in/recommended events: video URL.",
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
		"Flattened GA4 event parameter from built-in/recommended events: CSS classes of clicked link.",
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
		"Flattened GA4 event parameter from built-in/recommended events: domain of clicked link.",
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
		"Flattened GA4 event parameter from built-in/recommended events: ID attribute of clicked link.",
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
		"Flattened GA4 event parameter from built-in/recommended events: text content of clicked link.",
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
		"Flattened GA4 event parameter from built-in/recommended events: href URL of clicked link.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates if link is outbound (external).",
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
		"Flattened GA4 event parameter from built-in/recommended events: method name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: reason for cancellation.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates if error was fatal.",
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
		"Flattened GA4 event parameter from built-in/recommended events: Firebase error message.",
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
		"Flattened GA4 event parameter from built-in/recommended events: Firebase error value.",
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
		"Flattened GA4 event parameter from built-in/recommended events: Firebase screen name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: Firebase screen class.",
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
		"Flattened GA4 event parameter from built-in/recommended events: Firebase screen ID.",
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
		"Flattened GA4 event parameter from built-in/recommended events: previous Firebase screen name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: previous Firebase screen class.",
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
		"Flattened GA4 event parameter from built-in/recommended events: previous Firebase screen ID.",
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
		"Flattened GA4 event parameter from built-in/recommended events: message device time timestamp.",
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
		"Flattened GA4 event parameter from built-in/recommended events: message identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: message name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: message time timestamp.",
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
		"Flattened GA4 event parameter from built-in/recommended events: message type.",
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
		"Flattened GA4 event parameter from built-in/recommended events: topic.",
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
		"Flattened GA4 event parameter from built-in/recommended events: label.",
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
		"Flattened GA4 event parameter from built-in/recommended events: app version.",
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
		"Flattened GA4 event parameter from built-in/recommended events: previous app version.",
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
		"Flattened GA4 event parameter from built-in/recommended events: previous first open count.",
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
		"Flattened GA4 event parameter from built-in/recommended events: previous OS version.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates if updated with analytics.",
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
		"Flattened GA4 event parameter from built-in/recommended events: game achievement identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: game character name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: game level number.",
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
		"Flattened GA4 event parameter from built-in/recommended events: game level name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: game score.",
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
		"Flattened GA4 event parameter from built-in/recommended events: virtual currency name in games.",
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
		"Flattened GA4 event parameter from built-in/recommended events: item name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates success status.",
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
		"Flattened GA4 event parameter from built-in/recommended events: visibility status.",
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
		"Flattened GA4 event parameter from built-in/recommended events: screen resolution.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates if app is a system app.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates if this is a system app update.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates deferred analytics collection status.", // nolint:lll // it's a description
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
		"Flattened GA4 event parameter from built-in/recommended events: cause for analytics reset.",
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
		"Flattened GA4 event parameter from built-in/recommended events: previous Google Marketing Platform app ID.",
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
		"Flattened GA4 event parameter from built-in/recommended events: file extension.",
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
		"Flattened GA4 event parameter from built-in/recommended events: file name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: form destination URL.",
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
		"Flattened GA4 event parameter from built-in/recommended events: form ID.",
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
		"Flattened GA4 event parameter from built-in/recommended events: form name.",
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
		"Flattened GA4 event parameter from built-in/recommended events: form submit button text.",
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
		"Flattened GA4 event parameter from built-in/recommended events: group identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: language code.",
	),
)

var eventPercentScrolledColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPercentScrolled.ID,
	ProtocolInterfaces.EventParamPercentScrolled.Field,
	"ep.percent_scrolled",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamPercentScrolled.ID)),
	columns.WithEventColumnDocs(
		"Percent Scrolled",
		"Flattened GA4 event parameter from built-in/recommended events: percent of page scrolled.",
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
		"Flattened GA4 event parameter from built-in/recommended events: search term query.",
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
		"Flattened GA4 event parameter from built-in/recommended events: reason for lead unconversion.",
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
		"Flattened GA4 event parameter from built-in/recommended events: reason for lead disqualification.",
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
		"Flattened GA4 event parameter from built-in/recommended events: lead source.",
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
		"Flattened GA4 event parameter from built-in/recommended events: lead status.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates if product has free trial.",
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
		"Flattened GA4 event parameter from built-in/recommended events: indicates if product is a subscription.",
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
		"Flattened GA4 event parameter from built-in/recommended events: product identifier.",
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
		"Flattened GA4 event parameter from built-in/recommended events: product price.",
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
		"Flattened GA4 event parameter from built-in/recommended events: product quantity.",
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
		"Flattened GA4 event parameter from built-in/recommended events: introductory product price.",
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
		"GCLID (Param)",
		"Flattened GA4 event parameter from built-in/recommended events: Google Click ID.",
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
		"DCLID (Param)",
		"Flattened GA4 event parameter from built-in/recommended events: DoubleClick Click ID.",
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
		"SRSLTID (Param)",
		"Flattened GA4 event parameter from built-in/recommended events: Google Merchant Center click ID.",
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
		"ACLID (Param)",
		"Flattened GA4 event parameter from built-in/recommended events: Apple Search Ads click ID.",
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
		"ANID (Param)",
		"Flattened GA4 event parameter from built-in/recommended events: Apple Search Ads network ID.",
	),
)

var renewalCountParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamRenewalCount.ID,
	ProtocolInterfaces.EventParamRenewalCount.Field,
	"epn.renewal_count",
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamRenewalCount.ID)),
	columns.WithEventColumnDocs(
		"Renewal Count",
		"Flattened GA4 event parameter from built-in/recommended events: subscription renewal count.",
	),
)
