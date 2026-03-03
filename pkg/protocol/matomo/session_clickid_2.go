//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionClickIDFbclidColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClickIDFbclid.ID,
	columns.CoreInterfaces.SessionClickIDFbclid.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionClickIDMsclkidColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClickIDMsclkid.ID,
	columns.CoreInterfaces.SessionClickIDMsclkid.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
