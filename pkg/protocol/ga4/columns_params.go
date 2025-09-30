package ga4

import "github.com/d8a-tech/d8a/pkg/columns"

var eventContentGroupColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventContentGroup.ID,
	ProtocolInterfaces.EventContentGroup.Field,
	"ep.content_group",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventContentGroup.ID)),
	),
)

var eventContentIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventContentID.ID,
	ProtocolInterfaces.EventContentID.Field,
	"ep.content_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventContentID.ID))),
)

var eventContentTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventContentType.ID,
	ProtocolInterfaces.EventContentType.Field,
	"ep.content_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventContentType.ID)),
	),
)

var eventContentDescriptionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventContentDescription.ID,
	ProtocolInterfaces.EventContentDescription.Field,
	"ep.content",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventContentDescription.ID)),
	),
)

var eventCampaignColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCampaign.ID,
	ProtocolInterfaces.EventCampaign.Field,
	"ep.campaign",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCampaign.ID)),
	),
)

var eventCampaignIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCampaignID.ID,
	ProtocolInterfaces.EventCampaignID.Field,
	"ep.campaign_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCampaignID.ID)),
	),
)

var eventCampaignSourceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCampaignSource.ID,
	ProtocolInterfaces.EventCampaignSource.Field,
	"ep.campaign_source",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCampaignSource.ID)),
	),
)

var eventCampaignMediumColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCampaignMedium.ID,
	ProtocolInterfaces.EventCampaignMedium.Field,
	"ep.campaign_medium",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCampaignMedium.ID)),
	),
)

var eventCampaignContentColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCampaignContent.ID,
	ProtocolInterfaces.EventCampaignContent.Field,
	"ep.campaign_content",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCampaignContent.ID)),
	),
)

var eventCampaignTermColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCampaignTerm.ID,
	ProtocolInterfaces.EventCampaignTerm.Field,
	"ep.campaign_term",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCampaignTerm.ID)),
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventCouponColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCoupon.ID,
	ProtocolInterfaces.EventCoupon.Field,
	"ep.coupon",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCoupon.ID)),
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventCurrencyColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCurrency.ID,
	ProtocolInterfaces.EventCurrency.Field,
	"ep.currency",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCurrency.ID)),
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventShippingColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventShipping.ID,
	ProtocolInterfaces.EventShipping.Field,
	"ep.shipping",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventShipping.ID)),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventShippingTierColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventShippingTier.ID,
	ProtocolInterfaces.EventShippingTier.Field,
	"ep.shipping_tier",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventShippingTier.ID)),
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventPaymentTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPaymentType.ID,
	ProtocolInterfaces.EventPaymentType.Field,
	"ep.payment_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventPaymentType.ID)),
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventTaxColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventTax.ID,
	ProtocolInterfaces.EventTax.Field,
	"ep.tax",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventTax.ID)),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventTransactionIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventTransactionID.ID,
	ProtocolInterfaces.EventTransactionID.Field,
	"ep.transaction_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventTransactionID.ID)),
	),
)

// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
var eventValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventValue.ID,
	ProtocolInterfaces.EventValue.Field,
	"ep.value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventValue.ID)),
)

var eventItemListIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventItemListID.ID,
	ProtocolInterfaces.EventItemListID.Field,
	"ep.item_list_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventItemListID.ID)),
	),
)

var eventItemListNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventItemListName.ID,
	ProtocolInterfaces.EventItemListName.Field,
	"ep.item_list_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventItemListName.ID)),
	),
)

var eventCreativeNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCreativeName.ID,
	ProtocolInterfaces.EventCreativeName.Field,
	"ep.creative_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCreativeName.ID)),
	),
)

var eventCreativeSlotColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCreativeSlot.ID,
	ProtocolInterfaces.EventCreativeSlot.Field,
	"ep.creative_slot",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCreativeSlot.ID)),
	),
)

var eventPromotionIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPromotionID.ID,
	ProtocolInterfaces.EventPromotionID.Field,
	"ep.promotion_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventPromotionID.ID)),
	),
)

var eventPromotionNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventPromotionName.ID,
	ProtocolInterfaces.EventPromotionName.Field,
	"ep.promotion_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventPromotionName.ID)),
	),
)

// Ad related params (ad_exposure, ad_query, ad_impression, ad_reward)
var eventAdEventIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventAdEventID.ID,
	ProtocolInterfaces.EventAdEventID.Field,
	"ep.ad_event_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventAdEventID.ID)),
	),
)

var eventExposureTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventExposureTime.ID,
	ProtocolInterfaces.EventExposureTime.Field,
	"ep.exposure_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventExposureTime.ID)),
)

var eventAdUnitCodeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventAdUnitCode.ID,
	ProtocolInterfaces.EventAdUnitCode.Field,
	"ep.ad_unit_code",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventAdUnitCode.ID)),
	),
)

var eventRewardTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventRewardType.ID,
	ProtocolInterfaces.EventRewardType.Field,
	"ep.reward_type",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventRewardType.ID)),
	),
)

var eventRewardValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventRewardValue.ID,
	ProtocolInterfaces.EventRewardValue.Field,
	"ep.reward_value",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventRewardValue.ID)),
)

// Video params
var eventVideoCurrentTimeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVideoCurrentTime.ID,
	ProtocolInterfaces.EventVideoCurrentTime.Field,
	"ep.video_current_time",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventVideoCurrentTime.ID)),
)

var eventVideoDurationColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVideoDuration.ID,
	ProtocolInterfaces.EventVideoDuration.Field,
	"ep.video_duration",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventVideoDuration.ID)),
)

var eventVideoPercentColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVideoPercent.ID,
	ProtocolInterfaces.EventVideoPercent.Field,
	"ep.video_percent",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventVideoPercent.ID)),
)

var eventVideoProviderColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVideoProvider.ID,
	ProtocolInterfaces.EventVideoProvider.Field,
	"ep.video_provider",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventVideoProvider.ID)),
	),
)

var eventVideoTitleColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVideoTitle.ID,
	ProtocolInterfaces.EventVideoTitle.Field,
	"ep.video_title",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventVideoTitle.ID)),
	),
)

var eventVideoURLColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventVideoURL.ID,
	ProtocolInterfaces.EventVideoURL.Field,
	"ep.video_url",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventVideoURL.ID)),
	),
)

// EventLink columns for outbound click tracking
var eventLinkClassesColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLinkClasses.ID,
	ProtocolInterfaces.EventLinkClasses.Field,
	"ep.link_classes",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLinkClasses.ID)),
	),
)

var eventLinkDomainColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLinkDomain.ID,
	ProtocolInterfaces.EventLinkDomain.Field,
	"ep.link_domain",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLinkDomain.ID)),
	),
)

var eventLinkIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLinkID.ID,
	ProtocolInterfaces.EventLinkID.Field,
	"ep.link_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLinkID.ID)),
	),
)

var eventLinkTextColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLinkText.ID,
	ProtocolInterfaces.EventLinkText.Field,
	"ep.link_text",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLinkText.ID)),
	),
)

var eventLinkURLColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventLinkURL.ID,
	ProtocolInterfaces.EventLinkURL.Field,
	"ep.link_url",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventLinkURL.ID)),
	),
)

var eventOutboundColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventOutbound.ID,
	ProtocolInterfaces.EventOutbound.Field,
	"ep.outbound",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventOutbound.ID))),
)

// App params
var eventMethodColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMethod.ID,
	ProtocolInterfaces.EventMethod.Field,
	"ep.method",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventMethod.ID)),
	),
)

var eventCancellationReasonColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventCancellationReason.ID,
	ProtocolInterfaces.EventCancellationReason.Field,
	"ep.cancellation_reason",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventCancellationReason.ID)),
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
	ProtocolInterfaces.EventAppVersion.ID,
	ProtocolInterfaces.EventAppVersion.Field,
	"ep.app_version",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventAppVersion.ID)),
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
	"ep.score",
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
