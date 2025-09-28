package storage

import "testing"

func TestInMemoryKV(t *testing.T) {
	kv := NewInMemoryKV()
	KVTestSuite(t, kv)
}
func TestInMemorySet(t *testing.T) {
	set := NewInMemorySet()
	SetTestSuite(t, set)
}
