package matomo

import (
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

func repeatedSlotValueField(name string) *arrow.Field {
	return &arrow.Field{
		Name: name,
		Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "slot", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
		)),
		Nullable: true,
	}
}

type customDimension struct {
	slot  int64
	value string
}

func parseCustomDimensions(params url.Values) []customDimension {
	if len(params) == 0 {
		return nil
	}

	dimensions := make([]customDimension, 0)
	for key, values := range params {
		if !strings.HasPrefix(key, "dimension") {
			continue
		}

		slotPart := strings.TrimPrefix(key, "dimension")
		if slotPart == "" {
			continue
		}

		slot, err := strconv.ParseInt(slotPart, 10, 64)
		if err != nil {
			continue
		}

		if len(values) == 0 {
			continue
		}

		value := values[len(values)-1]
		if value == "" {
			continue
		}

		dimensions = append(dimensions, customDimension{slot: slot, value: value})
	}

	if len(dimensions) == 0 {
		return nil
	}

	sort.Slice(dimensions, func(i, j int) bool {
		return dimensions[i].slot < dimensions[j].slot
	})

	return dimensions
}

func customDimensionsToAny(dimensions []customDimension) []any {
	if len(dimensions) == 0 {
		return nil
	}

	out := make([]any, 0, len(dimensions))
	for _, dimension := range dimensions {
		out = append(out, map[string]any{
			"slot":  dimension.slot,
			"value": dimension.value,
		})
	}

	return out
}

var eventCustomDimensionsColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventCustomDimensions.ID,
	ProtocolInterfaces.EventCustomDimensions.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		parsed := parseCustomDimensions(event.BoundHit.MustParsedRequest().QueryParams)
		return customDimensionsToAny(parsed), nil
	},
	columns.WithEventColumnDocs(
		"Custom Dimensions",
		"Matomo custom dimensions from query parameters like dimension1, dimension2, and so on.",
	),
)

var sessionCustomDimensionsColumn = columns.NewSimpleSessionColumn(
	ProtocolInterfaces.SessionCustomDimensions.ID,
	ProtocolInterfaces.SessionCustomDimensions.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		mergedBySlot := make(map[int64]string)

		for _, event := range session.Events {
			for _, dimension := range parseCustomDimensions(event.BoundHit.MustParsedRequest().QueryParams) {
				mergedBySlot[dimension.slot] = dimension.value
			}
		}

		if len(mergedBySlot) == 0 {
			return nil, nil //nolint:nilnil // optional field
		}

		slots := make([]int64, 0, len(mergedBySlot))
		for slot := range mergedBySlot {
			slots = append(slots, slot)
		}
		sort.Slice(slots, func(i, j int) bool {
			return slots[i] < slots[j]
		})

		dimensions := make([]customDimension, 0, len(slots))
		for _, slot := range slots {
			dimensions = append(dimensions, customDimension{
				slot:  slot,
				value: mergedBySlot[slot],
			})
		}

		return customDimensionsToAny(dimensions), nil
	},
	columns.WithSessionColumnDocs(
		"Session Custom Dimensions",
		"Merged Matomo custom dimensions from query parameters like dimension1, dimension2, and so on across session events.",
	),
)
