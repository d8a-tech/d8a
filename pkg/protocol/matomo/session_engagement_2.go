//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionUniqueOutboundClicksColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUniqueOutboundClicks.ID,
	columns.CoreInterfaces.SessionUniqueOutboundClicks.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionTotalSiteSearchesColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalSiteSearches.ID,
	columns.CoreInterfaces.SessionTotalSiteSearches.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUniqueSiteSearchesColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUniqueSiteSearches.ID,
	columns.CoreInterfaces.SessionUniqueSiteSearches.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionTotalFormInteractionsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalFormInteractions.ID,
	columns.CoreInterfaces.SessionTotalFormInteractions.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUniqueFormInteractionsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUniqueFormInteractions.ID,
	columns.CoreInterfaces.SessionUniqueFormInteractions.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
