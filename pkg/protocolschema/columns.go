// Package protocolschema provides an integration between protocol and schema packages
package protocolschema

import (
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
)

type fromProtocolColumnsRegistry struct {
	protocolRegistry protocol.Registry
}

func (r *fromProtocolColumnsRegistry) Get(propertyID string) (schema.Columns, error) {
	protocol, err := r.protocolRegistry.Get(propertyID)
	if err != nil {
		return schema.Columns{}, err
	}
	protocolColumns := protocol.Columns()
	return protocolColumns, nil
}

// NewFromProtocolColumnsRegistry creates a new columns registry that gets columns from the protocol.
func NewFromProtocolColumnsRegistry(protocolRegistry protocol.Registry) schema.ColumnsRegistry {
	return &fromProtocolColumnsRegistry{
		protocolRegistry: protocolRegistry,
	}
}
