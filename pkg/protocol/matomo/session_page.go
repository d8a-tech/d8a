//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionEntryPageLocationColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionEntryPageLocation.ID,
	columns.CoreInterfaces.SessionEntryPageLocation.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionSecondPageLocationColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionSecondPageLocation.ID,
	columns.CoreInterfaces.SessionSecondPageLocation.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionExitPageLocationColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionExitPageLocation.ID,
	columns.CoreInterfaces.SessionExitPageLocation.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionEntryPageTitleColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionEntryPageTitle.ID,
	columns.CoreInterfaces.SessionEntryPageTitle.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sessionSecondPageTitleColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionSecondPageTitle.ID,
	columns.CoreInterfaces.SessionSecondPageTitle.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
