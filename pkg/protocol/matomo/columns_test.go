package matomo

import (
	"github.com/d8a-tech/d8a/pkg/protocol"
)

type staticPropertyIDExtractor struct {
	propertyID string
}

func (e *staticPropertyIDExtractor) PropertyID(_ *protocol.RequestContext) (string, error) {
	return e.propertyID, nil
}
