package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SessionMediumColumn only loads cached value computed from SessionSourceColumn, the logic is there
var SessionMediumColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionMedium.ID,
	columns.CoreInterfaces.SessionMedium.Field,
	0,
	func(e *schema.Event) (any, error) {
		return ReadSessionSourceMediumTerm(e).Medium, nil
	},
	func(e *schema.Event) bool { return true }, // first event is fine
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.SessionSource.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Medium",
		"Marketing channel (e.g., organic, cpc, social, email, referral) classifying how traffic arrived.", // nolint:lll // it's a description
	),
)
