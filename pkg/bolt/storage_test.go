package bolt

import (
	"os"
	"testing"

	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/sirupsen/logrus"
)

func TestBoltKV(t *testing.T) {
	// Create a temporary file for the database
	tmpFile, err := os.CreateTemp("", "test_kv_*.db")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	dbPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temporary file: %v", err)
	}
	defer func() {
		if err := os.Remove(dbPath); err != nil {
			logrus.Error("Failed to remove test database file: ", err)
		}
	}()

	kv, err := NewBoltKV(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BoltKV: %v", err)
	}

	// Run the KV test suite
	storage.KVTestSuite(t, kv)
}

func TestBoltSet(t *testing.T) {
	// Create a temporary file for the database
	tmpFile, err := os.CreateTemp("", "test_set_*.db")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	dbPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil { // Close the file as BoltDB will open it
		t.Fatalf("Failed to close temporary file: %v", err)
	}
	defer func() {
		if err := os.Remove(dbPath); err != nil {
			logrus.Error("Failed to remove test database file: ", err)
		}
	}()

	set, err := NewBoltSet(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BoltSet: %v", err)
	}

	// Run the Set test suite
	storage.SetTestSuite(t, set)
}
