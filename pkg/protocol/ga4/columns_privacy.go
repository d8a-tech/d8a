package ga4

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/sirupsen/logrus"
)

func parseBooleanFromQueryParamOrNilColumn(
	interfaceID schema.InterfaceID,
	field *arrow.Field,
	queryParam string,
	index int,
	options ...columns.EventColumnOptions,
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		interfaceID,
		field,
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			p := event.BoundHit.MustParsedRequest().QueryParams.Get(queryParam)
			if p == "" {
				return nil, nil // nolint:nilnil // nil is valid
			}
			if len(p) < index {
				logrus.Warnf(
					"%s: %s: index %d is out of bounds",
					interfaceID,
					p,
					index,
				)
				return nil, nil // nolint:nilnil // nil is valid
			}
			char := string(p[index])
			boolVal, err := util.StrToBool(char)
			if err != nil {
				logrus.Warnf(
					"%s: %v",
					interfaceID,
					err,
				)
				return nil, nil // nolint:nilnil // nil is valid
			}
			return boolVal, nil
		},
		options...,
	)
}

var eventPrivacyAnalyticsStorageColumn = parseBooleanFromQueryParamOrNilColumn(
	ProtocolInterfaces.EventPrivacyAnalyticsStorage.ID,
	ProtocolInterfaces.EventPrivacyAnalyticsStorage.Field,
	"gcs",
	3,
	columns.WithEventColumnDocs(
		"Privacy Analytics Storage",
		"Indicates whether the user has consented to analytics. Extracted from the Google Consent Settings (gcs) parameter. ", // nolint:lll // it's a description
	),
)

var eventPrivacyAdsStorageColumn = parseBooleanFromQueryParamOrNilColumn(
	ProtocolInterfaces.EventPrivacyAdsStorage.ID,
	ProtocolInterfaces.EventPrivacyAdsStorage.Field,
	"gcs",
	2,
	columns.WithEventColumnDocs(
		"Privacy Ads Storage",
		"Indicates whether the user has consented to advertising. Extracted from the Google Consent Settings (gcs) parameter.", // nolint:lll // it's a description
	),
)
