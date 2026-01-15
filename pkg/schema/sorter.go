package schema

import (
	"fmt"
	"sort"
	"strings"
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

	return NewColumns(sortedSessionColumns, sortedEventColumns, sortedSessionScopedEventColumns), nil
}

// sortColumns implements topological sort for column dependencies.
func (s *DependencySorter) sortColumns(columns []Column) ([]Column, error) {
	if len(columns) == 0 {
		return columns, nil
	}

	columnMap, inDegree, adjList, err := buildSortGraph(columns)
	if err != nil {
		return nil, err
	}

	result := topoSortColumns(columnMap, inDegree, adjList)

	// Check for cycles (should not happen if AssertAllDependenciesFulfilled passed)
	if len(result) != len(columns) {
		remaining := remainingIDs(inDegree)
		cycle := s.findCycle(buildReverseAdjList(columns, remaining), remaining)
		if len(cycle) > 0 {
			return nil, fmt.Errorf("circular dependency detected: %s", formatCyclePath(cycle))
		}
		return nil, fmt.Errorf("circular dependency detected among: %v", sortedIDs(remaining))
	}

	return result, nil
}

func buildSortGraph(columns []Column) (
	columnMap map[InterfaceID]Column,
	inDegree map[InterfaceID]int,
	adjList map[InterfaceID][]InterfaceID,
	err error,
) {
	columnMap, idSet, err := indexColumns(columns)
	if err != nil {
		return nil, nil, nil, err
	}
	inDegree = make(map[InterfaceID]int, len(columnMap))
	adjList = make(map[InterfaceID][]InterfaceID, len(columnMap))
	for id := range columnMap {
		inDegree[id] = 0
		adjList[id] = []InterfaceID{}
	}

	for _, col := range columns {
		colID := col.Implements().ID
		for _, dep := range col.DependsOn() {
			depID := dep.Interface
			if depID == colID {
				return nil, nil, nil, fmt.Errorf("invalid dependency: %s depends on itself", colID)
			}
			if !idSet[depID] {
				return nil, nil, nil, fmt.Errorf("missing dependency: %s depends on %s (not present)", colID, depID)
			}
			adjList[depID] = append(adjList[depID], colID)
			inDegree[colID]++
		}
	}
	return columnMap, inDegree, adjList, nil
}

func indexColumns(columns []Column) (columnMap map[InterfaceID]Column, idSet map[InterfaceID]bool, err error) {
	columnMap = make(map[InterfaceID]Column, len(columns))
	idSet = make(map[InterfaceID]bool, len(columns))
	idCount := make(map[InterfaceID]int, len(columns))

	for _, col := range columns {
		id := col.Implements().ID
		idCount[id]++
		if idCount[id] == 1 {
			columnMap[id] = col
			idSet[id] = true
		}
	}
	for id, count := range idCount {
		if count > 1 {
			return nil, nil, fmt.Errorf("duplicate column id: %s appears %d times", id, count)
		}
	}
	return columnMap, idSet, nil
}

func topoSortColumns(
	columnMap map[InterfaceID]Column,
	inDegree map[InterfaceID]int,
	adjList map[InterfaceID][]InterfaceID,
) []Column {
	queue := make([]InterfaceID, 0, len(inDegree))
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	sort.Slice(queue, func(i, j int) bool { return string(queue[i]) < string(queue[j]) })

	result := make([]Column, 0, len(inDegree))
	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]
		result = append(result, columnMap[currentID])

		newZero := make([]InterfaceID, 0)
		for _, neighborID := range adjList[currentID] {
			inDegree[neighborID]--
			if inDegree[neighborID] == 0 {
				newZero = append(newZero, neighborID)
			}
		}

		sort.Slice(newZero, func(i, j int) bool { return string(newZero[i]) < string(newZero[j]) })
		queue = append(queue, newZero...)
	}

	return result
}

func remainingIDs(inDegree map[InterfaceID]int) map[InterfaceID]bool {
	remaining := make(map[InterfaceID]bool)
	for id, degree := range inDegree {
		if degree > 0 {
			remaining[id] = true
		}
	}
	return remaining
}

func buildReverseAdjList(columns []Column, remaining map[InterfaceID]bool) map[InterfaceID][]InterfaceID {
	reverseAdjList := make(map[InterfaceID][]InterfaceID)
	for _, col := range columns {
		colID := col.Implements().ID
		if !remaining[colID] {
			continue
		}
		for _, dep := range col.DependsOn() {
			if remaining[dep.Interface] {
				reverseAdjList[colID] = append(reverseAdjList[colID], dep.Interface)
			}
		}
	}
	return reverseAdjList
}

func sortedIDs(ids map[InterfaceID]bool) []InterfaceID {
	out := make([]InterfaceID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out
}

func formatCyclePath(cycle []InterfaceID) string {
	parts := make([]string, 0, len(cycle))
	for _, id := range cycle {
		parts = append(parts, string(id))
	}
	return strings.Join(parts, " -> ")
}

// findCycle finds a cycle in the dependency graph using DFS.
// Returns the cycle path as a slice of InterfaceIDs, or nil if no cycle found.
func (s *DependencySorter) findCycle(
	reverseAdjList map[InterfaceID][]InterfaceID,
	remaining map[InterfaceID]bool,
) []InterfaceID {
	visited := make(map[InterfaceID]bool)
	recStack := make(map[InterfaceID]bool)
	nodes := sortedIDs(remaining)

	for _, node := range nodes {
		if visited[node] {
			continue
		}
		if cycle := s.findCycleFrom(node, reverseAdjList, remaining, visited, recStack, nil); len(cycle) > 0 {
			return cycle
		}
	}
	return nil
}

func (s *DependencySorter) findCycleFrom(
	node InterfaceID,
	reverseAdjList map[InterfaceID][]InterfaceID,
	remaining map[InterfaceID]bool,
	visited map[InterfaceID]bool,
	recStack map[InterfaceID]bool,
	path []InterfaceID,
) []InterfaceID {
	if !remaining[node] {
		return nil
	}
	if recStack[node] {
		return cycleFromPath(path, node)
	}
	if visited[node] {
		return nil
	}

	visited[node] = true
	recStack[node] = true
	nextPath := appendPath(path, node)

	neighbors := reverseAdjList[node]
	sort.Slice(neighbors, func(i, j int) bool { return string(neighbors[i]) < string(neighbors[j]) })
	for _, neighbor := range neighbors {
		if cycle := s.findCycleFrom(neighbor, reverseAdjList, remaining, visited, recStack, nextPath); len(cycle) > 0 {
			return cycle
		}
	}

	recStack[node] = false
	return nil
}

func appendPath(path []InterfaceID, node InterfaceID) []InterfaceID {
	// Avoid `appendAssign` gocritic warning by not using append on a different slice.
	next := make([]InterfaceID, len(path)+1)
	copy(next, path)
	next[len(path)] = node
	return next
}

func cycleFromPath(path []InterfaceID, node InterfaceID) []InterfaceID {
	startIdx := -1
	for i, id := range path {
		if id == node {
			startIdx = i
			break
		}
	}
	if startIdx < 0 {
		return nil
	}
	cycle := make([]InterfaceID, 0, len(path)-startIdx+1)
	cycle = append(cycle, path[startIdx:]...)
	cycle = append(cycle, node)
	return cycle
}
