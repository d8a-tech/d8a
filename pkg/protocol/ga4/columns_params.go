package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var eventContentGroupColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamContentGroup.ID,
	ProtocolInterfaces.EventParamContentGroup.Field,
	"ep.content_group",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamContentGroup.ID)),
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
)

var eventContentTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamContentType.ID,
	ProtocolInterfaces.EventParamContentType.Field,
	"ep.content_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamContentType.ID)),
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
)

var eventCampaignColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaign.ID,
	ProtocolInterfaces.EventParamCampaign.Field,
	"ep.campaign",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaign.ID)),
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
)

var eventCampaignSourceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaignSource.ID,
	ProtocolInterfaces.EventParamCampaignSource.Field,
	"ep.campaign_source",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaignSource.ID)),
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
)

var eventCampaignContentColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCampaignContent.ID,
	ProtocolInterfaces.EventParamCampaignContent.Field,
	"ep.campaign_content",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCampaignContent.ID)),
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
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventShippingColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamShipping.ID,
	ProtocolInterfaces.EventParamShipping.Field,
	"epn.shipping",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamShipping.ID)),
)

// On surface duplicates the above - nevertheless it's in the dataform, so including it for now
// I guess the reasoning that is the former contains raw param value, while the latter
// draws some conclusions, like using zero value if the param is empty
var eventShippingValueColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventShippingValue.ID,
	ProtocolInterfaces.EventShippingValue.Field,
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
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventTaxColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamTax.ID,
	ProtocolInterfaces.EventParamTax.Field,
	"epn.tax",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamTax.ID)),
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
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamValue.ID,
	ProtocolInterfaces.EventParamValue.Field,
	"epn.value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamValue.ID)),
)

var eventItemListIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamItemListID.ID,
	ProtocolInterfaces.EventParamItemListID.Field,
	"ep.item_list_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamItemListID.ID)),
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
)

var eventCreativeNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCreativeName.ID,
	ProtocolInterfaces.EventParamCreativeName.Field,
	"ep.creative_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCreativeName.ID)),
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
)

var eventPromotionIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPromotionID.ID,
	ProtocolInterfaces.EventParamPromotionID.Field,
	"ep.promotion_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPromotionID.ID)),
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
)

var eventExposureTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamExposureTime.ID,
	ProtocolInterfaces.EventParamExposureTime.Field,
	"ep.exposure_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamExposureTime.ID)),
)

var eventAdUnitCodeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAdUnitCode.ID,
	ProtocolInterfaces.EventParamAdUnitCode.Field,
	"ep.ad_unit_code",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAdUnitCode.ID)),
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
)

var eventRewardValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamRewardValue.ID,
	ProtocolInterfaces.EventParamRewardValue.Field,
	"epn.reward_value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamRewardValue.ID)),
)

// Video params
var eventVideoCurrentTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoCurrentTime.ID,
	ProtocolInterfaces.EventParamVideoCurrentTime.Field,
	"epn.video_current_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamVideoCurrentTime.ID)),
)

var eventVideoDurationColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoDuration.ID,
	ProtocolInterfaces.EventParamVideoDuration.Field,
	"epn.video_duration",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamVideoDuration.ID)),
)

var eventVideoPercentColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoPercent.ID,
	ProtocolInterfaces.EventParamVideoPercent.Field,
	"ep.video_percent",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamVideoPercent.ID)),
)

var eventVideoProviderColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoProvider.ID,
	ProtocolInterfaces.EventParamVideoProvider.Field,
	"ep.video_provider",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamVideoProvider.ID)),
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
)

var eventVideoURLColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVideoURL.ID,
	ProtocolInterfaces.EventParamVideoURL.Field,
	"ep.video_url",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamVideoURL.ID)),
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
)

var eventLinkDomainColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLinkDomain.ID,
	ProtocolInterfaces.EventParamLinkDomain.Field,
	"ep.link_domain",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLinkDomain.ID)),
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
)

var eventLinkTextColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLinkText.ID,
	ProtocolInterfaces.EventParamLinkText.Field,
	"ep.link_text",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLinkText.ID)),
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
)

var eventOutboundColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamOutbound.ID,
	ProtocolInterfaces.EventParamOutbound.Field,
	"ep.outbound",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamOutbound.ID))),
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
)

var eventCancellationReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCancellationReason.ID,
	ProtocolInterfaces.EventParamCancellationReason.Field,
	"ep.cancellation_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCancellationReason.ID)),
	),
)

var eventFatalColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFatal.ID,
	ProtocolInterfaces.EventParamFatal.Field,
	"ep.fatal",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamFatal.ID))),
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
)

var eventFirebaseErrorValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebaseErrorValue.ID,
	ProtocolInterfaces.EventParamFirebaseErrorValue.Field,
	"ep.firebase_error_value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebaseErrorValue.ID)),
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
)

var eventFirebaseScreenClassColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebaseScreenClass.ID,
	ProtocolInterfaces.EventParamFirebaseScreenClass.Field,
	"ep.firebase_screen_class",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebaseScreenClass.ID)),
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
)

var eventFirebasePreviousScreenColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebasePreviousScreen.ID,
	ProtocolInterfaces.EventParamFirebasePreviousScreen.Field,
	"ep.firebase_previous_screen",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebasePreviousScreen.ID)),
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
)

var eventFirebasePreviousIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFirebasePreviousID.ID,
	ProtocolInterfaces.EventParamFirebasePreviousID.Field,
	"ep.firebase_previous_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFirebasePreviousID.ID)),
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
)

var eventMessageIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageID.ID,
	ProtocolInterfaces.EventParamMessageID.Field,
	"ep.message_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamMessageID.ID)),
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
)

var eventMessageTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageTime.ID,
	ProtocolInterfaces.EventParamMessageTime.Field,
	"ep.message_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamMessageTime.ID)),
)

var eventMessageTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamMessageType.ID,
	ProtocolInterfaces.EventParamMessageType.Field,
	"ep.message_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamMessageType.ID)),
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
)

var eventLabelColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLabel.ID,
	ProtocolInterfaces.EventParamLabel.Field,
	"ep.label",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLabel.ID)),
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
)

var eventPreviousAppVersionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousAppVersion.ID,
	ProtocolInterfaces.EventParamPreviousAppVersion.Field,
	"ep.previous_app_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPreviousAppVersion.ID)),
	),
)

var eventPreviousFirstOpenCountColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousFirstOpenCount.ID,
	ProtocolInterfaces.EventParamPreviousFirstOpenCount.Field,
	"ep.previous_first_open_count",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamPreviousFirstOpenCount.ID)),
)

var eventPreviousOSVersionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousOSVersion.ID,
	ProtocolInterfaces.EventParamPreviousOSVersion.Field,
	"ep.previous_os_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPreviousOSVersion.ID)),
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
)

var eventCharacterColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamCharacter.ID,
	ProtocolInterfaces.EventParamCharacter.Field,
	"ep.character",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamCharacter.ID)),
	),
)

var eventLevelColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLevel.ID,
	ProtocolInterfaces.EventParamLevel.Field,
	"ep.level",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamLevel.ID)),
)

var eventLevelNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLevelName.ID,
	ProtocolInterfaces.EventParamLevelName.Field,
	"ep.level_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLevelName.ID)),
	),
)

var eventScoreColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamScore.ID,
	ProtocolInterfaces.EventParamScore.Field,
	"epn.score",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamScore.ID)),
)

var eventVirtualCurrencyNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVirtualCurrencyName.ID,
	ProtocolInterfaces.EventParamVirtualCurrencyName.Field,
	"ep.virtual_currency_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamVirtualCurrencyName.ID)),
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
)

var eventSuccessColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSuccess.ID,
	ProtocolInterfaces.EventParamSuccess.Field,
	"ep.success",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSuccess.ID))),
)

// System params - automatically collected with app events
var eventVisibleColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamVisible.ID,
	ProtocolInterfaces.EventParamVisible.Field,
	"ep.visible",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamVisible.ID))),
)

var eventScreenResolutionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamScreenResolution.ID,
	ProtocolInterfaces.EventParamScreenResolution.Field,
	"ep.screen_resolution",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamScreenResolution.ID)),
	),
)

var eventSystemAppColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSystemApp.ID,
	ProtocolInterfaces.EventParamSystemApp.Field,
	"ep.system_app",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSystemApp.ID))),
)

var eventSystemAppUpdateColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSystemAppUpdate.ID,
	ProtocolInterfaces.EventParamSystemAppUpdate.Field,
	"ep.system_app_update",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSystemAppUpdate.ID))),
)

var eventDeferredAnalyticsCollectionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamDeferredAnalyticsCollection.ID,
	ProtocolInterfaces.EventParamDeferredAnalyticsCollection.Field,
	"ep.deferred_analytics_collection",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamDeferredAnalyticsCollection.ID)),
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
)

var eventPreviousGmpAppIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPreviousGmpAppID.ID,
	ProtocolInterfaces.EventParamPreviousGmpAppID.Field,
	"ep.previous_gmp_app_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamPreviousGmpAppID.ID)),
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
)

var eventFileNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFileName.ID,
	ProtocolInterfaces.EventParamFileName.Field,
	"ep.file_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFileName.ID)),
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
)

var eventFormIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFormID.ID,
	ProtocolInterfaces.EventParamFormID.Field,
	"ep.form_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFormID.ID)),
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
)

var eventFormSubmitTextColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFormSubmitText.ID,
	ProtocolInterfaces.EventParamFormSubmitText.Field,
	"ep.form_submit_text",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamFormSubmitText.ID)),
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
)

var eventLanguageColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLanguage.ID,
	ProtocolInterfaces.EventParamLanguage.Field,
	"ep.language",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLanguage.ID)),
	),
)

var eventPercentScrolledColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPercentScrolled.ID,
	ProtocolInterfaces.EventParamPercentScrolled.Field,
	"ep.percent_scrolled",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamPercentScrolled.ID)),
)

var eventSearchTermColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSearchTerm.ID,
	ProtocolInterfaces.EventParamSearchTerm.Field,
	"ep.search_term",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamSearchTerm.ID)),
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
)

var eventDisqualifiedLeadReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamDisqualifiedLeadReason.ID,
	ProtocolInterfaces.EventParamDisqualifiedLeadReason.Field,
	"ep.disqualified_lead_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamDisqualifiedLeadReason.ID)),
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
)

var eventLeadStatusColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamLeadStatus.ID,
	ProtocolInterfaces.EventParamLeadStatus.Field,
	"ep.lead_status",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamLeadStatus.ID)),
	),
)

var eventFreeTrialColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamFreeTrial.ID,
	ProtocolInterfaces.EventParamFreeTrial.Field,
	"ep.free_trial",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamFreeTrial.ID))),
)

var eventSubscriptionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSubscription.ID,
	ProtocolInterfaces.EventParamSubscription.Field,
	"ep.subscription",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventParamSubscription.ID))),
)

var eventProductIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamProductID.ID,
	ProtocolInterfaces.EventParamProductID.Field,
	"ep.product_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamProductID.ID)),
	),
)

var eventPriceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamPrice.ID,
	ProtocolInterfaces.EventParamPrice.Field,
	"epn.price",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamPrice.ID),
	),
)

var eventQuantityColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamQuantity.ID,
	ProtocolInterfaces.EventParamQuantity.Field,
	"epn.quantity",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamQuantity.ID),
	),
)

var eventIntroductoryPriceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamIntroductoryPrice.ID,
	ProtocolInterfaces.EventParamIntroductoryPrice.Field,
	"epn.introductory_price",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamIntroductoryPrice.ID),
	),
)

var gclidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamGclid.ID,
	ProtocolInterfaces.EventParamGclid.Field,
	"ep.gclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamGclid.ID)),
	),
)

var dclidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamDclid.ID,
	ProtocolInterfaces.EventParamDclid.Field,
	"ep.dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamDclid.ID)),
	),
)

var srsltidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamSrsltid.ID,
	ProtocolInterfaces.EventParamSrsltid.Field,
	"ep.srsltid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamSrsltid.ID)),
	),
)

var aclidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAclid.ID,
	ProtocolInterfaces.EventParamAclid.Field,
	"ep.aclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAclid.ID)),
	),
)

var anidParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamAnid.ID,
	ProtocolInterfaces.EventParamAnid.Field,
	"ep.anid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamAnid.ID)),
	),
)

var renewalCountParamColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamRenewalCount.ID,
	ProtocolInterfaces.EventParamRenewalCount.Field,
	"epn.renewal_count",
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamRenewalCount.ID)),
)
