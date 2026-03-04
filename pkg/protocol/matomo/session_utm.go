//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionUtmCampaignColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmCampaign.ID,
	columns.CoreInterfaces.SessionUtmCampaign.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmSourceColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmSource.ID,
	columns.CoreInterfaces.SessionUtmSource.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmMediumColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmMedium.ID,
	columns.CoreInterfaces.SessionUtmMedium.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmContentColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmContent.ID,
	columns.CoreInterfaces.SessionUtmContent.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmTermColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmTerm.ID,
	columns.CoreInterfaces.SessionUtmTerm.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmIDColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmID.ID,
	columns.CoreInterfaces.SessionUtmID.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmSourcePlatformColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmSourcePlatform.ID,
	columns.CoreInterfaces.SessionUtmSourcePlatform.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmCreativeFormatColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmCreativeFormat.ID,
	columns.CoreInterfaces.SessionUtmCreativeFormat.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUtmMarketingTacticColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUtmMarketingTactic.ID,
	columns.CoreInterfaces.SessionUtmMarketingTactic.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
