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

func customVariablesByNameFromParsed(parsed []any) map[string]string {
	if len(parsed) == 0 {
		return nil
	}

	merged := make(map[string]string, len(parsed))
	for _, item := range parsed {
		variable, ok := item.(map[string]any)
		if !ok {
			continue
		}

		name, ok := variable["name"].(string)
		if !ok {
			continue
		}

		value, ok := variable["value"].(string)
		if !ok {
			continue
		}

		merged[name] = value
	}

	if len(merged) == 0 {
		return nil
	}

	return merged
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

var sessionCustomVariablesColumn = columns.NewSimpleSessionColumn(
	ProtocolInterfaces.SessionCustomVariables.ID,
	ProtocolInterfaces.SessionCustomVariables.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		mergedByName := make(map[string]string)

		for _, event := range session.Events {
			parsed := parseCustomVariables(event.BoundHit.MustParsedRequest().QueryParams.Get("_cvar"))
			for name, value := range customVariablesByNameFromParsed(parsed) {
				mergedByName[name] = value
			}
		}

		if len(mergedByName) == 0 {
			return nil, nil //nolint:nilnil // optional field
		}

		names := make([]string, 0, len(mergedByName))
		for name := range mergedByName {
			names = append(names, name)
		}
		sort.Strings(names)

		out := make([]any, 0, len(names))
		for _, name := range names {
			out = append(out, map[string]any{
				"name":  name,
				"value": mergedByName[name],
			})
		}

		return out, nil
	},
	columns.WithSessionColumnDocs(
		"Session Custom Variables",
		"Merged Matomo custom variables from the _cvar query parameter across session events.",
	),
)
