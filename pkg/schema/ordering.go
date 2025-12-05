package schema

import (
	"math"
	"reflect" //nolint:depguard // sorting will be used only during initialization
	"sort"
)

// OrderKeeper is a interface that can be used to get the order of a given interface ID.
type OrderKeeper interface {
	GetOrder(id InterfaceID) int
}

// InterfaceOrdering maintains the order of column interfaces based on their definition position in structs.
type InterfaceOrdering struct {
	order map[InterfaceID]int
}

// NewInterfaceDefinitionOrderKeeper creates a new OrderKeeper instance, that tracks
// the order of columns using the definition position in structs.
func NewInterfaceDefinitionOrderKeeper(structs ...any) *InterfaceOrdering {
	ordering := &InterfaceOrdering{
		order: make(map[InterfaceID]int),
	}
	for _, s := range structs {
		ordering.registerInterfaceStruct(s)
	}
	return ordering
}

// RegisterInterfaceStruct walks through a struct containing Interface fields and registers their order.
func (o *InterfaceOrdering) registerInterfaceStruct(s any) {
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

type noParticicularOrderKeeper struct {
	orders  map[InterfaceID]int
	current int
}

func (o *noParticicularOrderKeeper) GetOrder(id InterfaceID) int {
	has, ok := o.orders[id]
	if !ok {
		has = o.current
		o.current++
		o.orders[id] = has
	}
	return has
}

// NewNoParticicularOrderKeeper creates a new OrderKeeper that assigns an order
// to each interface ID in the order they are encountered, useful for testing or
// quick prototyping.
func NewNoParticicularOrderKeeper() OrderKeeper {
	return &noParticicularOrderKeeper{
		orders:  make(map[InterfaceID]int),
		current: 0,
	}
}

// SortByOrdering sorts a slice of columns according to their interface order.
func SortByOrdering[T Column](cols []T, ordering OrderKeeper) []T {
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
func Sorted(columns Columns, ordering OrderKeeper) Columns {
	newCols := columns.Copy()
	newCols.Session = SortByOrdering(newCols.Session, ordering)
	newCols.Event = SortByOrdering(newCols.Event, ordering)
	newCols.SessionScopedEvent = SortByOrdering(newCols.SessionScopedEvent, ordering)
	return newCols
}
