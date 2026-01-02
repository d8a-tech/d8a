// Package hits provides functionality for handling and processing tracking hits
package hits

import (
	"net/http"
	"net/url"
	"time"
	"unsafe"

	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// ClientID represents an ID stored client-side. Be it cookie or device ID.
type ClientID string

type ProtocolAttributes struct {
	AuthoritativeClientID ClientID `cbor:"ai"`
	ClientID              ClientID `cbor:"ci"`
	EventName             string   `cbor:"en"`
	PropertyID            string   `cbor:"pi"`
	UserID                *string  `cbor:"uid"`
}

type Request struct {
	IP                 string      `cbor:"ip"`
	Host               string      `cbor:"h"`
	ServerReceivedTime time.Time   `cbor:"srt"`
	QueryParams        url.Values  `cbor:"qp"`
	Body               []byte      `cbor:"bd"`
	Path               string      `cbor:"p"`
	Method             string      `cbor:"m"`
	Headers            http.Header `cbor:"he"`
}

func (s *Request) Clone() *Request {
	queryParamsCopy := url.Values{}
	for key, values := range s.QueryParams {
		queryParamsCopy[key] = make([]string, len(values))
		copy(queryParamsCopy[key], values)
	}
	return &Request{
		IP:                 s.IP,
		Host:               s.Host,
		ServerReceivedTime: s.ServerReceivedTime,
		QueryParams:        queryParamsCopy,
		Body:               s.Body,
		Path:               s.Path,
		Method:             s.Method,
		Headers:            s.Headers.Clone(),
	}
}

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
	EventName             string   `cbor:"en"`
	// PropertyID uses GA nomenclature. In other than GA protocols, it holds the analogous concept, like app, website, etc
	PropertyID string  `cbor:"pi"`
	UserID     *string `cbor:"uid"`

	Metadata map[string]string `cbor:"md"`
	Request  *Request          `cbor:"sa"`
}

// SessionStamp returns a unique identifier for the session
func (h *Hit) SessionStamp() string {
	directSessionStamp := h.Request.QueryParams.Get("sessionStamp")
	if directSessionStamp != "" {
		return directSessionStamp
	}
	return h.Request.IP
}

func (h *Hit) MustServerAttributes() *Request {
	if h.Request == nil {
		logrus.Errorf("server attributes are nil for hit %s, that should not happen", h.ID)
		h.Request = &Request{
			Headers:            http.Header{},
			QueryParams:        url.Values{},
			ServerReceivedTime: time.Now(),
		}
	}
	return h.Request
}

// New creates a new Hit with random ID and current time
func New() *Hit {
	clientID := uuid.New().String()
	return &Hit{
		Metadata: map[string]string{},
		Request: &Request{
			Headers:            http.Header{},
			QueryParams:        url.Values{},
			ServerReceivedTime: time.Now(),
		},
		ID:                    uuid.New().String(),
		ClientID:              ClientID(clientID),
		AuthoritativeClientID: ClientID(clientID),
	}
}

// NewWithServerAttributes creates a new Hit with the given ServerAttributes
func NewWithServerAttributes(serverAttributes *Request) *Hit {
	clientID := uuid.New().String()
	return &Hit{
		Metadata:              map[string]string{},
		Request:               serverAttributes,
		ID:                    uuid.New().String(),
		ClientID:              ClientID(clientID),
		AuthoritativeClientID: ClientID(clientID),
	}
}

// Size returns the total byte size of the Hit struct including all its fields.
// This includes the struct itself, all string data, slice data, and map overhead.
func (h *Hit) Size() uint32 {
	var size uint32

	// Base struct size (includes all pointers and fixed-size fields)
	size += util.SafeIntToUint32(int(unsafe.Sizeof(*h)))

	// String data sizes (strings in Go store data separately from the struct)
	size += util.SafeIntToUint32(len(h.ID))
	size += util.SafeIntToUint32(len(h.AuthoritativeClientID))
	size += util.SafeIntToUint32(len(h.ClientID))
	size += util.SafeIntToUint32(len(h.PropertyID))
	size += util.SafeIntToUint32(len(h.MustServerAttributes().IP))
	size += util.SafeIntToUint32(len(h.MustServerAttributes().Host))
	size += util.SafeIntToUint32(len(h.MustServerAttributes().Path))
	size += util.SafeIntToUint32(len(h.MustServerAttributes().Method))

	// time.Time is 24 bytes (3 int64 fields), already included in unsafe.Sizeof

	// Slice data size
	size += util.SafeIntToUint32(len(h.Request.Body))

	// UserID pointer and data
	if h.UserID != nil {
		size += util.SafeIntToUint32(len(*h.UserID))
	}

	// QueryParams size (url.Values is map[string][]string)
	for key, values := range h.Request.QueryParams {
		size += util.SafeIntToUint32(len(key))
		for _, value := range values {
			size += util.SafeIntToUint32(len(value))
		}
	}

	// Headers size (url.Values is map[string][]string)
	for key, values := range h.Request.Headers {
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
	hitCopy.Request = h.Request.Clone()

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
