//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionTotalVideoEngagementsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalVideoEngagements.ID,
	columns.CoreInterfaces.SessionTotalVideoEngagements.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionTotalFileDownloadsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalFileDownloads.ID,
	columns.CoreInterfaces.SessionTotalFileDownloads.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionUniqueFileDownloadsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUniqueFileDownloads.ID,
	columns.CoreInterfaces.SessionUniqueFileDownloads.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
