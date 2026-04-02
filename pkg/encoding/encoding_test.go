package encoding

import (
	"bytes"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCBOREncoderDecoder_RoundTrip(t *testing.T) {
	type payload struct {
		Name   string
		Count  int
		Values []string
	}

	original := payload{
		Name:   "session",
		Count:  3,
		Values: []string{"a", "b", "c"},
	}

	var buf bytes.Buffer
	written, err := CBOREncoder(&buf, original)
	require.NoError(t, err)
	assert.Equal(t, len(buf.Bytes()), written)

	var decoded payload
	err = CBORDecoder(bytes.NewReader(buf.Bytes()), &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestCBORDecoder_RoundTripHitPointerShape(t *testing.T) {
	userID := "user-1"
	original := &hits.Hit{
		ID:                    "hit-1",
		AuthoritativeClientID: hits.ClientID("acid-1"),
		ClientID:              hits.ClientID("cid-1"),
		EventName:             "page_view",
		PropertyID:            "property-1",
		UserID:                &userID,
		Metadata: map[string]string{
			"source": "test",
		},
		Request: &hits.ParsedRequest{
			IP:                 "127.0.0.1",
			Host:               "example.com",
			ServerReceivedTime: time.Unix(1_700_000_000, 0).UTC(),
			Path:               "/collect",
			Method:             "POST",
			Body:               []byte("body"),
		},
	}

	var buf bytes.Buffer
	_, err := CBOREncoder(&buf, original)
	require.NoError(t, err)

	var decodedHit *hits.Hit
	err = CBORDecoder(bytes.NewReader(buf.Bytes()), &decodedHit)
	require.NoError(t, err)
	require.NotNil(t, decodedHit)
	assert.Equal(t, original.ID, decodedHit.ID)
	assert.Equal(t, original.AuthoritativeClientID, decodedHit.AuthoritativeClientID)
	assert.Equal(t, original.ClientID, decodedHit.ClientID)
	assert.Equal(t, original.EventName, decodedHit.EventName)
	assert.Equal(t, original.PropertyID, decodedHit.PropertyID)
	assert.Equal(t, original.UserID, decodedHit.UserID)
	assert.Equal(t, original.Metadata, decodedHit.Metadata)
	require.NotNil(t, decodedHit.Request)
	assert.Equal(t, original.Request.IP, decodedHit.Request.IP)
	assert.Equal(t, original.Request.Host, decodedHit.Request.Host)
	assert.Equal(t, original.Request.ServerReceivedTime.UnixNano(), decodedHit.Request.ServerReceivedTime.UnixNano())
	assert.Equal(t, original.Request.Path, decodedHit.Request.Path)
	assert.Equal(t, original.Request.Method, decodedHit.Request.Method)
	assert.Equal(t, original.Request.Body, decodedHit.Request.Body)
}
