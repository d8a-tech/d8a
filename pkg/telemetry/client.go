package telemetry

import (
	"sync"

	"github.com/google/uuid"
)

var (
	clientIDOnce sync.Once
	clientID     string
)

// ClientIDGeneratedOnStartup returns a function that generates and returns client ID stored in memory.
// New client ID is generated on each app restart (in-memory only).
func ClientIDGeneratedOnStartup() func() string {
	return func() string {
		clientIDOnce.Do(func() {
			clientID = uuid.New().String()
		})
		return clientID
	}
}
