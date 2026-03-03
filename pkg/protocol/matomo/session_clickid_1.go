//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionClickIDGclidColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClickIDGclid.ID,
	columns.CoreInterfaces.SessionClickIDGclid.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionClickIDDclidColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClickIDDclid.ID,
	columns.CoreInterfaces.SessionClickIDDclid.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionClickIDGbraidColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClickIDGbraid.ID,
	columns.CoreInterfaces.SessionClickIDGbraid.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionClickIDSrsltidColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClickIDSrsltid.ID,
	columns.CoreInterfaces.SessionClickIDSrsltid.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionClickIDWbraidColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClickIDWbraid.ID,
	columns.CoreInterfaces.SessionClickIDWbraid.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
