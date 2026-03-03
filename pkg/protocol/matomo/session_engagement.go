//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionTotalPageViewsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalPageViews.ID,
	columns.CoreInterfaces.SessionTotalPageViews.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUniquePageViewsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUniquePageViews.ID,
	columns.CoreInterfaces.SessionUniquePageViews.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionTotalPurchasesColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalPurchases.ID,
	columns.CoreInterfaces.SessionTotalPurchases.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionTotalScrollsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalScrolls.ID,
	columns.CoreInterfaces.SessionTotalScrolls.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionTotalOutboundClicksColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalOutboundClicks.ID,
	columns.CoreInterfaces.SessionTotalOutboundClicks.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
