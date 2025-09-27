package receiver

import (
	"github.com/d8a-tech/d8a/pkg/hits"
)

// Storage is a storage interface for storing hits
type Storage interface {
	Push([]*hits.Hit) error
}
