package dbip

import (
	"sync"

	"github.com/oschwald/maxminddb-golang/v2"
)

var registry = map[string]*maxminddb.Reader{}
var registryMutex = sync.Mutex{}

// GetMMDB returns a cached maxminddb.Reader for the given MMDB path.
func GetMMDB(mmdbPath string) (*maxminddb.Reader, error) {
	var err error
	mmdb, ok := registry[mmdbPath]
	if ok {
		return mmdb, nil
	}
	registryMutex.Lock()
	defer registryMutex.Unlock()
	mmdb, ok = registry[mmdbPath]
	if ok {
		return mmdb, nil
	}
	mmdb, err = maxminddb.Open(mmdbPath)
	if err != nil {
		return nil, err
	}
	registry[mmdbPath] = mmdb
	return mmdb, nil
}
