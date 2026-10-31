package splitter

import (
	"github.com/d8a-tech/d8a/pkg/expressions"
	"github.com/expr-lang/expr"
)

// FunctionOptions preserves the traffic-filter expression function contract.
func FunctionOptions() []expr.Option {
	return expressions.FunctionOptions()
}
