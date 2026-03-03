//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionExitPageTitleColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionExitPageTitle.ID,
	columns.CoreInterfaces.SessionExitPageTitle.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// TODO(matomo): implement
		return nil, nil
	},
)
