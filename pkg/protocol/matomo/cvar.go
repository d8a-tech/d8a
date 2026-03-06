package matomo

import (
	"encoding/json"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

func repeatedNameValueField(name string) *arrow.Field {
	return &arrow.Field{
		Name: name,
		Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
		)),
		Nullable: true,
	}
}

type customVariable struct {
	slot  string
	name  string
	value string
}

func parseCustomVariables(raw string) []any {
	if raw == "" {
		return nil
	}

	var payload map[string][]string
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}

	if len(payload) == 0 {
		return nil
	}

	variables := make([]customVariable, 0, len(payload))
	for slot, values := range payload {
		if len(values) != 2 {
			continue
		}

		variables = append(variables, customVariable{
			slot:  slot,
			name:  values[0],
			value: values[1],
		})
	}

	if len(variables) == 0 {
		return nil
	}

	sort.Slice(variables, func(i, j int) bool {
		leftIndex, leftErr := strconv.Atoi(variables[i].slot)
		rightIndex, rightErr := strconv.Atoi(variables[j].slot)
		if leftErr == nil && rightErr == nil {
			return leftIndex < rightIndex
		}
		if leftErr == nil {
			return true
		}
		if rightErr == nil {
			return false
		}

		return variables[i].slot < variables[j].slot
	})

	out := make([]any, 0, len(variables))
	for _, variable := range variables {
		out = append(out, map[string]any{
			"name":  variable.name,
			"value": variable.value,
		})
	}

	return out
}

var eventCustomVariablesColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventCustomVariables.ID,
	ProtocolInterfaces.EventCustomVariables.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		return parseCustomVariables(event.BoundHit.MustParsedRequest().QueryParams.Get("cvar")), nil
	},
	columns.WithEventColumnDocs(
		"Custom Variables",
		"Matomo custom variables from the cvar query parameter.",
	),
)
