package schema

import (
	"math"
	"reflect"
	"sort"
)

// InterfaceOrdering maintains the order of column interfaces based on their definition position in structs.
type InterfaceOrdering struct {
	order map[InterfaceID]int
}

// NewInterfaceOrdering creates a new InterfaceOrdering instance.
func NewInterfaceOrdering(structs ...interface{}) *InterfaceOrdering {
	ordering := &InterfaceOrdering{
		order: make(map[InterfaceID]int),
	}
	for _, s := range structs {
		ordering.registerInterfaceStruct(s)
	}
	return ordering
}

// RegisterInterfaceStruct walks through a struct containing Interface fields and registers their order.
func (o *InterfaceOrdering) registerInterfaceStruct(s interface{}) {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Type() == reflect.TypeOf(Interface{}) {
			if intf, ok := field.Interface().(Interface); ok {
				// Only register if not already registered (first registration wins)
				if _, exists := o.order[intf.ID]; !exists {
					o.order[intf.ID] = len(o.order)
				}
			}
		}
	}
}

// GetOrder returns the order index for a given interface ID.
// Returns math.MaxInt for unknown interfaces (they sort last).
func (o *InterfaceOrdering) GetOrder(id InterfaceID) int {
	if order, ok := o.order[id]; ok {
		return order
	}
	return math.MaxInt
}

// SortByOrdering sorts a slice of columns according to their interface order.
func SortByOrdering[T Column](cols []T, ordering *InterfaceOrdering) []T {
	if ordering == nil {
		return cols
	}

	sorted := make([]T, len(cols))
	copy(sorted, cols)
	sort.SliceStable(sorted, func(i, j int) bool {
		orderI := ordering.GetOrder(sorted[i].Implements().ID)
		orderJ := ordering.GetOrder(sorted[j].Implements().ID)
		return orderI < orderJ
	})
	return sorted
}

// Sorted sorts a slice of columns according to their interface order.
func Sorted(columns Columns, ordering *InterfaceOrdering) Columns {
	return Columns{
		Session:            SortByOrdering(columns.Session, ordering),
		Event:              SortByOrdering(columns.Event, ordering),
		SessionScopedEvent: SortByOrdering(columns.SessionScopedEvent, ordering),
	}
}
