package eventcolumns

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// UtmMarketingTacticColumn is the column for the UTM marketing tactic of an event
var UtmMarketingTacticColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmMarketingTactic.ID,
	columns.CoreInterfaces.EventUtmMarketingTactic.Field,
	"utm_marketing_tactic",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmMarketingTactic.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Marketing Tactic",
		"The marketing tactic from the 'utm_marketing_tactic' URL parameter, describes the targeting criteria (e.g., 'remarketing', 'prospecting').", // nolint:lll // it's a description
	),
)

// UtmSourcePlatformColumn is the column for the UTM source platform of an event
var UtmSourcePlatformColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmSourcePlatform.ID,
	columns.CoreInterfaces.EventUtmSourcePlatform.Field,
	"utm_source_platform",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmSourcePlatform.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Source Platform",
		"The source platform from the 'utm_source_platform' URL parameter, identifies the platform of the traffic source (e.g., 'Google Ads', 'Display & Video 360', 'Meta Ads').", // nolint:lll // it's a description
	),
)

// UtmTermColumn is the column for the UTM term of an event
var UtmTermColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmTerm.ID,
	columns.CoreInterfaces.EventUtmTerm.Field,
	"utm_term",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmTerm.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Term",
		"The search term from the 'utm_term' URL parameter, used primarily for paid search campaigns to identify the keywords that triggered the ad.", // nolint:lll // it's a description
	),
)

// UtmContentColumn is the column for the UTM content of an event
var UtmContentColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmContent.ID,
	columns.CoreInterfaces.EventUtmContent.Field,
	"utm_content",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmContent.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Content",
		"The content identifier from the 'utm_content' URL parameter, used to differentiate similar content or links within the same campaign (e.g., 'banner_top', 'button_cta').", // nolint:lll // it's a description
	),
)

// UtmSourceColumn is the column for the UTM source of an event
var UtmSourceColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmSource.ID,
	columns.CoreInterfaces.EventUtmSource.Field,
	"utm_source",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmSource.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Source",
		"The traffic source from the 'utm_source' URL parameter, identifies where the traffic originated (e.g., 'google', 'facebook', 'newsletter').", // nolint:lll // it's a description
	),
)

// UtmMediumColumn is the column for the UTM medium of an event
var UtmMediumColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmMedium.ID,
	columns.CoreInterfaces.EventUtmMedium.Field,
	"utm_medium",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmMedium.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Medium",
		"The traffic medium from the 'utm_medium' URL parameter, identifies the marketing medium (e.g., 'cpc', 'email', 'social', 'organic').", // nolint:lll // it's a description
	),
)

// UtmCampaignColumn is the column for the UTM campaign of an event
var UtmCampaignColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmCampaign.ID,
	columns.CoreInterfaces.EventUtmCampaign.Field,
	"utm_campaign",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmCampaign.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Campaign",
		"The campaign name from the 'utm_campaign' URL parameter, used to identify specific marketing campaigns (e.g., 'summer_sale', 'product_launch_2024').", // nolint:lll // it's a description
	),
)

// UtmIDColumn is the column for the UTM ID of an event
var UtmIDColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmID.ID,
	columns.CoreInterfaces.EventUtmID.Field,
	"utm_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmID.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM ID",
		"The campaign ID from the 'utm_id' URL parameter, used to identify a specific campaign with a unique identifier for integration with advertising platforms.", // nolint:lll // it's a description
	),
)

// UtmCreativeFormatColumn is the column for the UTM creative format of an event
var UtmCreativeFormatColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmCreativeFormat.ID,
	columns.CoreInterfaces.EventUtmCreativeFormat.Field,
	"utm_creative_format",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmCreativeFormat.ID)),
	),
	columns.WithEventColumnDocs(
		"UTM Creative Format",
		"The creative format from the 'utm_creative_format' URL parameter, identifies the format of the creative asset.", // nolint:lll // it's a description
	),
)

// UtmSourceMediumColumn is the column for the UTM source medium of an event
var UtmSourceMediumColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventUtmSourceMedium.ID,
	columns.CoreInterfaces.EventUtmSourceMedium.Field,
	func(e *schema.Event) (any, error) {
		utmSource, ok := e.Values[columns.CoreInterfaces.EventUtmSource.Field.Name]
		if !ok {
			return "", nil
		}
		utmSourceStr, ok := utmSource.(string)
		if !ok {
			return "", nil
		}
		utmMedium, ok := e.Values[columns.CoreInterfaces.EventUtmMedium.Field.Name]
		if !ok {
			return "", nil
		}
		utmMediumStr, ok := utmMedium.(string)
		if !ok {
			return "", nil
		}
		return fmt.Sprintf("%s/%s", utmSourceStr, utmMediumStr), nil
	},
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmSourceMedium.ID)),
	),
	columns.WithEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmSource.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmMedium.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithEventColumnDocs("UTM source / medium", "Combined traffic source and medium in 'source/medium' format (e.g., 'google/cpc', 'newsletter/email'). Standard analytics dimension for attribution reporting."), //nolint:lll // it's a description
)
