// Package hits provides functionality for handling and processing tracking hits
package hits

import (
	"net/url"
	"time"

	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/google/uuid"
)

// ClientID represents an ID stored client-side. Be it cookie or device ID.
type ClientID string

// Hit stores information about a single `/collect` endpoint call
type Hit struct {
	// Random UUID of every hit.
	ID string `cbor:"i"`
	// AuthoritativeClientID is the ID of first hit of a proto-session the current hit belongs to.
	// In normal cases this will be the same as ClientID, but if for example another client ID
	// will be mappend by a session stamp, the AuthoritativeClientID will change to the ClientID of the hit
	// that claimed the session stamp.
	// This ID should not be used for reporting and is only internal detail of the tracker.
	AuthoritativeClientID ClientID `cbor:"ai"`
	ClientID              ClientID `cbor:"ci"`
	// PropertyID uses GA nomenclature. In other than GA protocols, it holds the analogous concept, like app, website, etc
	PropertyID         string            `cbor:"pi"`
	IP                 string            `cbor:"ip"`
	Host               string            `cbor:"h"`
	ServerReceivedTime string            `cbor:"srt"`
	QueryParams        url.Values        `cbor:"qp"`
	Body               []byte            `cbor:"bd"`
	Path               string            `cbor:"p"`
	Method             string            `cbor:"m"`
	Headers            url.Values        `cbor:"he"`
	Metadata           map[string]string `cbor:"md"`
	Timestamp          time.Time         `cbor:"ts"`
	UserID             *string           `cbor:"uid"`
}

// SessionStamp returns a unique identifier for the session
func (h *Hit) SessionStamp() string {
	directSessionStamp := h.QueryParams.Get("sessionStamp")
	if directSessionStamp != "" {
		return directSessionStamp
	}
	return h.IP
}

// New creates a new Hit with random ID and current time
func New() *Hit {
	clientID := uuid.New().String()
	return &Hit{
		Metadata:              map[string]string{},
		Headers:               url.Values{},
		QueryParams:           url.Values{},
		ID:                    uuid.New().String(),
		ClientID:              ClientID(clientID),
		AuthoritativeClientID: ClientID(clientID),
		Timestamp:             time.Unix(0, 0).UTC(),
		ServerReceivedTime:    time.Now().Format(time.RFC3339),
	}
}

// Size returns the total byte size of the Hit struct including all its fields
func (h *Hit) Size() uint32 {
	var size uint32

	// Fixed size fields
	size += util.SafeIntToUint32(len(h.ID))
	size += util.SafeIntToUint32(len(h.AuthoritativeClientID))
	size += util.SafeIntToUint32(len(h.ClientID))
	size += util.SafeIntToUint32(len(h.IP))
	size += util.SafeIntToUint32(len(h.Host))
	size += util.SafeIntToUint32(len(h.ServerReceivedTime))
	size += util.SafeIntToUint32(len(h.Body))
	size += util.SafeIntToUint32(len(h.Path))
	size += util.SafeIntToUint32(len(h.Method))

	// QueryParams size
	for key, values := range h.QueryParams {
		size += util.SafeIntToUint32(len(key))
		for _, value := range values {
			size += util.SafeIntToUint32(len(value))
		}
	}

	// Headers size
	for key, values := range h.Headers {
		size += util.SafeIntToUint32(len(key))
		for _, value := range values {
			size += util.SafeIntToUint32(len(value))
		}
	}

	// Metadata size
	for key, value := range h.Metadata {
		size += util.SafeIntToUint32(len(key))
		size += util.SafeIntToUint32(len(value))
	}

	return size
}

// Copy creates a deep copy of the Hit
func (h *Hit) Copy() Hit {
	// Create a copy of the base struct
	hitCopy := *h

	// Deep copy url.Values for QueryParams
	if h.QueryParams != nil {
		hitCopy.QueryParams = make(url.Values)
		for key, values := range h.QueryParams {
			hitCopy.QueryParams[key] = make([]string, len(values))
			copy(hitCopy.QueryParams[key], values)
		}
	}

	// Deep copy url.Values for Headers
	if h.Headers != nil {
		hitCopy.Headers = make(url.Values)
		for key, values := range h.Headers {
			hitCopy.Headers[key] = make([]string, len(values))
			copy(hitCopy.Headers[key], values)
		}
	}

	// Deep copy Metadata map
	if h.Metadata != nil {
		hitCopy.Metadata = make(map[string]string)
		for key, value := range h.Metadata {
			hitCopy.Metadata[key] = value
		}
	}

	// Deep copy UserID pointer
	if h.UserID != nil {
		userIDCopy := *h.UserID
		hitCopy.UserID = &userIDCopy
	}

	return hitCopy
}
