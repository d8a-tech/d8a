//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sseTimeOnPageColumn = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSETimeOnPage.ID,
	columns.CoreInterfaces.SSETimeOnPage.Field,
	func(session *schema.Session, i int) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sseIsEntryPageColumn = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSEIsEntryPage.ID,
	columns.CoreInterfaces.SSEIsEntryPage.Field,
	func(session *schema.Session, i int) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)

var sseIsExitPageColumn = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSEIsExitPage.ID,
	columns.CoreInterfaces.SSEIsExitPage.Field,
	func(session *schema.Session, i int) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
