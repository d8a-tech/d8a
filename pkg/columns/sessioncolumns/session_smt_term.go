package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SessionTermColumn only loads cached value computed from SessionSourceColumn, the logic is there
var SessionTermColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionTerm.ID,
	columns.CoreInterfaces.SessionTerm.Field,
	0,
	func(e *schema.Event) (any, error) {
		return ReadSessionSourceMediumTerm(e).Term, nil
	},
	func(e *schema.Event) bool { return true }, // first event is fine
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.SessionSource.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Term",
		"Search keyword or campaign term extracted from referrer query parameters or UTM tags. For details, see the D8A documentation on traffic attribution.", // nolint:lll // it's a description
	),
)
