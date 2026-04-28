package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionReturningUserColumn = columns.NewSimpleSessionColumn(
	ProtocolInterfaces.SessionReturningUser.ID,
	ProtocolInterfaces.SessionReturningUser.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		if len(session.Events) == 0 {
			return false, nil
		}

		idn := session.Events[0].BoundHit.MustParsedRequest().QueryParams.Get("_idn")
		if idn == "0" {
			return true, nil
		}

		return false, nil
	},
	columns.WithSessionColumnRequired(false),
	columns.WithSessionColumnDocs(
		"Session Returning User",
		"Returning user indicator derived from Matomo _idn on the first event in the session. "+
			"Set to true when _idn=0, otherwise false.",
	),
)
