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
	columns.WithEventColumnCast(columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamContentID.ID))),
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
	ProtocolInterfaces.EventFatal.ID,
	ProtocolInterfaces.EventFatal.Field,
	"ep.fatal",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventFatal.ID))),
)

// Firebase params
var eventFirebaseErrorColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebaseError.ID,
	ProtocolInterfaces.EventFirebaseError.Field,
	"ep.firebase_error",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebaseError.ID)),
	),
)

var eventFirebaseErrorValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebaseErrorValue.ID,
	ProtocolInterfaces.EventFirebaseErrorValue.Field,
	"ep.firebase_error_value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebaseErrorValue.ID)),
	),
)

var eventFirebaseScreenColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebaseScreen.ID,
	ProtocolInterfaces.EventFirebaseScreen.Field,
	"ep.firebase_screen",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebaseScreen.ID)),
	),
)

var eventFirebaseScreenClassColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebaseScreenClass.ID,
	ProtocolInterfaces.EventFirebaseScreenClass.Field,
	"ep.firebase_screen_class",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebaseScreenClass.ID)),
	),
)

var eventFirebaseScreenIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebaseScreenID.ID,
	ProtocolInterfaces.EventFirebaseScreenID.Field,
	"ep.firebase_screen_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebaseScreenID.ID)),
	),
)

var eventFirebasePreviousScreenColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebasePreviousScreen.ID,
	ProtocolInterfaces.EventFirebasePreviousScreen.Field,
	"ep.firebase_previous_screen",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebasePreviousScreen.ID)),
	),
)

var eventFirebasePreviousClassColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebasePreviousClass.ID,
	ProtocolInterfaces.EventFirebasePreviousClass.Field,
	"ep.firebase_previous_class",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebasePreviousClass.ID)),
	),
)

var eventFirebasePreviousIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFirebasePreviousID.ID,
	ProtocolInterfaces.EventFirebasePreviousID.Field,
	"ep.firebase_previous_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFirebasePreviousID.ID)),
	),
)

// Message params - used in firebase_in_app_message_(action|dismiss|impression), fiam_(action|dismiss|impression)
// notification_(foreground|open|dismiss|receive)
var eventMessageDeviceTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMessageDeviceTime.ID,
	ProtocolInterfaces.EventMessageDeviceTime.Field,
	"ep.message_device_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventMessageDeviceTime.ID)),
)

var eventMessageIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMessageID.ID,
	ProtocolInterfaces.EventMessageID.Field,
	"ep.message_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventMessageID.ID)),
	),
)

var eventMessageNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMessageName.ID,
	ProtocolInterfaces.EventMessageName.Field,
	"ep.message_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventMessageName.ID)),
	),
)

var eventMessageTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMessageTime.ID,
	ProtocolInterfaces.EventMessageTime.Field,
	"ep.message_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventMessageTime.ID)),
)

var eventMessageTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMessageType.ID,
	ProtocolInterfaces.EventMessageType.Field,
	"ep.message_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventMessageType.ID)),
	),
)

var eventTopicColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventTopic.ID,
	ProtocolInterfaces.EventTopic.Field,
	"ep.topic",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventTopic.ID)),
	),
)

var eventLabelColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLabel.ID,
	ProtocolInterfaces.EventLabel.Field,
	"ep.label",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLabel.ID)),
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
	ProtocolInterfaces.EventPreviousAppVersion.ID,
	ProtocolInterfaces.EventPreviousAppVersion.Field,
	"ep.previous_app_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventPreviousAppVersion.ID)),
	),
)

var eventPreviousFirstOpenCountColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPreviousFirstOpenCount.ID,
	ProtocolInterfaces.EventPreviousFirstOpenCount.Field,
	"ep.previous_first_open_count",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventPreviousFirstOpenCount.ID)),
)

var eventPreviousOSVersionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPreviousOSVersion.ID,
	ProtocolInterfaces.EventPreviousOSVersion.Field,
	"ep.previous_os_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventPreviousOSVersion.ID)),
	),
)

var eventUpdatedWithAnalyticsColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventUpdatedWithAnalytics.ID,
	ProtocolInterfaces.EventUpdatedWithAnalytics.Field,
	"ep.updated_with_analytics",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventUpdatedWithAnalytics.ID))),
)

// Gaming params - used in gaming events
var eventAchievementIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventAchievementID.ID,
	ProtocolInterfaces.EventAchievementID.Field,
	"ep.achievement_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventAchievementID.ID)),
	),
)

var eventCharacterColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCharacter.ID,
	ProtocolInterfaces.EventCharacter.Field,
	"ep.character",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCharacter.ID)),
	),
)

var eventLevelColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLevel.ID,
	ProtocolInterfaces.EventLevel.Field,
	"ep.level",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventLevel.ID)),
)

var eventLevelNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLevelName.ID,
	ProtocolInterfaces.EventLevelName.Field,
	"ep.level_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLevelName.ID)),
	),
)

var eventScoreColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventScore.ID,
	ProtocolInterfaces.EventScore.Field,
	"epn.score",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventScore.ID)),
)

var eventVirtualCurrencyNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVirtualCurrencyName.ID,
	ProtocolInterfaces.EventVirtualCurrencyName.Field,
	"ep.virtual_currency_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventVirtualCurrencyName.ID)),
	),
)

var eventItemNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventItemName.ID,
	ProtocolInterfaces.EventItemName.Field,
	"ep.item_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventItemName.ID)),
	),
)

var eventSuccessColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventSuccess.ID,
	ProtocolInterfaces.EventSuccess.Field,
	"ep.success",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventSuccess.ID))),
)

// System params - automatically collected with app events
var eventVisibleColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVisible.ID,
	ProtocolInterfaces.EventVisible.Field,
	"ep.visible",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventVisible.ID))),
)

var eventScreenResolutionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventScreenResolution.ID,
	ProtocolInterfaces.EventScreenResolution.Field,
	"ep.screen_resolution",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventScreenResolution.ID)),
	),
)

var eventSystemAppColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventSystemApp.ID,
	ProtocolInterfaces.EventSystemApp.Field,
	"ep.system_app",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventSystemApp.ID))),
)

var eventSystemAppUpdateColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventSystemAppUpdate.ID,
	ProtocolInterfaces.EventSystemAppUpdate.Field,
	"ep.system_app_update",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventSystemAppUpdate.ID))),
)

var eventDeferredAnalyticsCollectionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventDeferredAnalyticsCollection.ID,
	ProtocolInterfaces.EventDeferredAnalyticsCollection.Field,
	"ep.deferred_analytics_collection",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventDeferredAnalyticsCollection.ID)),
	),
)

var eventResetAnalyticsCauseColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventResetAnalyticsCause.ID,
	ProtocolInterfaces.EventResetAnalyticsCause.Field,
	"ep.reset_analytics_cause",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventResetAnalyticsCause.ID)),
	),
)

var eventPreviousGmpAppIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPreviousGmpAppID.ID,
	ProtocolInterfaces.EventPreviousGmpAppID.Field,
	"ep.previous_gmp_app_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventPreviousGmpAppID.ID)),
	),
)

// Form and file params - used in form and file events
var eventFileExtensionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFileExtension.ID,
	ProtocolInterfaces.EventFileExtension.Field,
	"ep.file_extension",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFileExtension.ID)),
	),
)

var eventFileNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFileName.ID,
	ProtocolInterfaces.EventFileName.Field,
	"ep.file_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFileName.ID)),
	),
)

var eventFormDestinationColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFormDestination.ID,
	ProtocolInterfaces.EventFormDestination.Field,
	"ep.form_destination",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFormDestination.ID)),
	),
)

var eventFormIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFormID.ID,
	ProtocolInterfaces.EventFormID.Field,
	"ep.form_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFormID.ID)),
	),
)

var eventFormNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFormName.ID,
	ProtocolInterfaces.EventFormName.Field,
	"ep.form_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFormName.ID)),
	),
)

var eventFormSubmitTextColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFormSubmitText.ID,
	ProtocolInterfaces.EventFormSubmitText.Field,
	"ep.form_submit_text",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventFormSubmitText.ID)),
	),
)

// Engagement params

var eventGroupIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventGroupID.ID,
	ProtocolInterfaces.EventGroupID.Field,
	"ep.group_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventGroupID.ID)),
	),
)

var eventLanguageColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLanguage.ID,
	ProtocolInterfaces.EventLanguage.Field,
	"ep.language",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLanguage.ID)),
	),
)

var eventPercentScrolledColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPercentScrolled.ID,
	ProtocolInterfaces.EventPercentScrolled.Field,
	"ep.percent_scrolled",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventPercentScrolled.ID)),
)

var eventSearchTermColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventSearchTerm.ID,
	ProtocolInterfaces.EventSearchTerm.Field,
	"ep.search_term",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSearchTerm.ID)),
	),
)

// Lead params

var eventUnconvertLeadReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventUnconvertLeadReason.ID,
	ProtocolInterfaces.EventUnconvertLeadReason.Field,
	"ep.unconvert_lead_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventUnconvertLeadReason.ID)),
	),
)

var eventDisqualifiedLeadReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventDisqualifiedLeadReason.ID,
	ProtocolInterfaces.EventDisqualifiedLeadReason.Field,
	"ep.disqualified_lead_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventDisqualifiedLeadReason.ID)),
	),
)

var eventLeadSourceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLeadSource.ID,
	ProtocolInterfaces.EventLeadSource.Field,
	"ep.lead_source",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLeadSource.ID)),
	),
)

var eventLeadStatusColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLeadStatus.ID,
	ProtocolInterfaces.EventLeadStatus.Field,
	"ep.lead_status",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLeadStatus.ID)),
	),
)

var eventFreeTrialColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventFreeTrial.ID,
	ProtocolInterfaces.EventFreeTrial.Field,
	"ep.free_trial",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventFreeTrial.ID))),
)

var eventSubscriptionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventSubscription.ID,
	ProtocolInterfaces.EventSubscription.Field,
	"ep.subscription",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventSubscription.ID))),
)

var eventProductIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventProductID.ID,
	ProtocolInterfaces.EventProductID.Field,
	"ep.product_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventProductID.ID)),
	),
)

var eventPriceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPrice.ID,
	ProtocolInterfaces.EventPrice.Field,
	"epn.price",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventPrice.ID),
	),
)

var eventQuantityColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventQuantity.ID,
	ProtocolInterfaces.EventQuantity.Field,
	"epn.quantity",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventQuantity.ID),
	),
)

var eventIntroductoryPriceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventIntroductoryPrice.ID,
	ProtocolInterfaces.EventIntroductoryPrice.Field,
	"epn.introductory_price",
	columns.WithEventColumnCast(
		columns.CastToFloat64OrNil(ProtocolInterfaces.EventIntroductoryPrice.ID),
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
