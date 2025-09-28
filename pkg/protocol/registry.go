package protocol

// Registry allows to get a protocol for a given property ID.
type Registry interface {
	Get(propertyID string) (Protocol, error)
}

type staticProtocolRegistry struct {
	protocols       map[string]Protocol
	defaultProtocol Protocol
}

func (r *staticProtocolRegistry) Get(propertyID string) (Protocol, error) {
	protocol, ok := r.protocols[propertyID]
	if !ok {
		return r.defaultProtocol, nil
	}
	return protocol, nil
}

// NewStaticRegistry creates a new static protocol registry.
func NewStaticRegistry(protocols map[string]Protocol, defaultProtocol Protocol) Registry {
	return &staticProtocolRegistry{
		protocols:       protocols,
		defaultProtocol: defaultProtocol,
	}
}
