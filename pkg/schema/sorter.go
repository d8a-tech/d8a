package schema

import (
	"fmt"
	"sort"
)

// DependencySorter provides topological sorting functionality for columns with dependencies.
type DependencySorter struct{}

// NewDependencySorter creates a new dependency sorter.
func NewDependencySorter() *DependencySorter {
	return &DependencySorter{}
}

// SortSessionColumns sorts session columns based on their dependencies.
func (s *DependencySorter) SortSessionColumns(columns []SessionColumn) ([]SessionColumn, error) {
	genericColumns := ToGenericColumns(columns)
	sortedGeneric, err := s.sortColumns(genericColumns)
	if err != nil {
		return nil, err
	}

	// Convert back to SessionColumn
	sorted := make([]SessionColumn, len(sortedGeneric))
	for i, col := range sortedGeneric {
		sessionCol, ok := col.(SessionColumn)
		if !ok {
			return nil, fmt.Errorf("expected SessionColumn, got %T", col)
		}
		sorted[i] = sessionCol
	}
	return sorted, nil
}

// SortEventColumns sorts event columns based on their dependencies.
func (s *DependencySorter) SortEventColumns(columns []EventColumn) ([]EventColumn, error) {
	genericColumns := ToGenericColumns(columns)
	sortedGeneric, err := s.sortColumns(genericColumns)
	if err != nil {
		return nil, err
	}

	// Convert back to EventColumn
	sorted := make([]EventColumn, len(sortedGeneric))
	for i, col := range sortedGeneric {
		eventCol, ok := col.(EventColumn)
		if !ok {
			return nil, fmt.Errorf("expected EventColumn, got %T", col)
		}
		sorted[i] = eventCol
	}
	return sorted, nil
}

// SortAllColumns sorts both session and event columns together, respecting cross-type dependencies.
func (s *DependencySorter) SortAllColumns(columns Columns) (Columns, error) {
	// Combine all columns for sorting
	allColumns := make([]Column, 0, len(columns.Session)+len(columns.Event)+len(columns.SessionScopedEvent))

	// Add session columns
	for _, col := range columns.Session {
		allColumns = append(allColumns, col)
	}

	// Add event columns
	for _, col := range columns.Event {
		allColumns = append(allColumns, col)
	}

	// Add session-scoped event columns
	for _, col := range columns.SessionScopedEvent {
		allColumns = append(allColumns, col)
	}

	// Sort all columns together
	sortedAll, err := s.sortColumns(allColumns)
	if err != nil {
		return Columns{}, err
	}

	// Separate back into slices by type
	var sortedSessionColumns []SessionColumn
	var sortedEventColumns []EventColumn
	var sortedSessionScopedEventColumns []SessionScopedEventColumn

	for _, col := range sortedAll {
		switch c := col.(type) {
		case SessionColumn:
			sortedSessionColumns = append(sortedSessionColumns, c)
		case EventColumn:
			sortedEventColumns = append(sortedEventColumns, c)
		case SessionScopedEventColumn:
			sortedSessionScopedEventColumns = append(sortedSessionScopedEventColumns, c)
		default:
			return Columns{}, fmt.Errorf("unknown column type: %T", col)
		}
	}

	return NewColumns3(sortedSessionColumns, sortedEventColumns, sortedSessionScopedEventColumns), nil
}

// sortColumns implements topological sort for column dependencies.
func (s *DependencySorter) sortColumns(columns []Column) ([]Column, error) {
	if len(columns) == 0 {
		return columns, nil
	}

	// Create adjacency list and in-degree count
	columnMap := make(map[InterfaceID]Column)
	inDegree := make(map[InterfaceID]int)
	adjList := make(map[InterfaceID][]InterfaceID)

	// Initialize structures
	for _, col := range columns {
		id := col.Implements().ID
		columnMap[id] = col
		inDegree[id] = 0
		adjList[id] = []InterfaceID{}
	}

	// Build dependency graph
	for _, col := range columns {
		for _, dep := range col.DependsOn() {
			// Add edge from dependency to dependent
			adjList[dep.Interface] = append(adjList[dep.Interface], col.Implements().ID)
			inDegree[col.Implements().ID]++
		}
	}

	// Topological sort using Kahn's algorithm
	var queue []InterfaceID
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	// Sort the initial queue for deterministic ordering
	sort.Slice(queue, func(i, j int) bool {
		return string(queue[i]) < string(queue[j])
	})

	var result []Column
	for len(queue) > 0 {
		// Remove vertex with no incoming edges
		currentID := queue[0]
		queue = queue[1:]
		result = append(result, columnMap[currentID])

		// Collect neighbors that will have their in-degree reduced to 0
		var newZeroDegreeNodes []InterfaceID

		// Remove edges from current vertex
		for _, neighborID := range adjList[currentID] {
			inDegree[neighborID]--
			if inDegree[neighborID] == 0 {
				newZeroDegreeNodes = append(newZeroDegreeNodes, neighborID)
			}
		}

		// Sort new zero-degree nodes for deterministic ordering
		sort.Slice(newZeroDegreeNodes, func(i, j int) bool {
			return string(newZeroDegreeNodes[i]) < string(newZeroDegreeNodes[j])
		})

		// Add to queue in sorted order
		queue = append(queue, newZeroDegreeNodes...)
	}

	// Check for cycles (should not happen if AssertAllDependenciesFulfilled passed)
	if len(result) != len(columns) {
		return nil, fmt.Errorf("circular dependency detected")
	}

	return result, nil
}
