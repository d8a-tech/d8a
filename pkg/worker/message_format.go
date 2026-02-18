package worker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/util"
)

// MessageFormat defines the interface for serializing and deserializing tasks
// Can be optionally used by Consumer and Publisher implementations, it's not a requirement though
type MessageFormat interface {
	// Serialize converts a task into binary format
	Serialize(t *Task) ([]byte, error)
	// Deserialize converts binary data into a task
	Deserialize(data []byte) (*Task, error)
}

// binaryMessageFormat implements MessageFormat using a binary format
type binaryMessageFormat struct {
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc
}

// BinaryMessageFormatOption configures binaryMessageFormat.
type BinaryMessageFormatOption func(*binaryMessageFormat)

// WithEncoderDecoderPair sets custom encoder and decoder for headers serialization.
func WithEncoderDecoderPair(enc encoding.EncoderFunc, dec encoding.DecoderFunc) BinaryMessageFormatOption {
	return func(f *binaryMessageFormat) {
		f.encoder = enc
		f.decoder = dec
	}
}

// NewBinaryMessageFormat creates a new binary message format implementation
func NewBinaryMessageFormat(opts ...BinaryMessageFormatOption) MessageFormat {
	f := &binaryMessageFormat{
		encoder: encoding.JSONEncoder,
		decoder: encoding.JSONDecoder,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Serialize implements MessageFormat interface
func (f *binaryMessageFormat) Serialize(t *Task) ([]byte, error) {
	if len(t.Type) > 255 {
		return nil, errors.New("task type string too long")
	}

	var buf bytes.Buffer
	// Write length byte
	// #nosec G115 - length checked to be <= 255 above
	if err := buf.WriteByte(byte(len(t.Type))); err != nil {
		return nil, fmt.Errorf("error writing type length: %w", err)
	}
	// Write type string
	if _, err := buf.WriteString(t.Type); err != nil {
		return nil, fmt.Errorf("error writing type: %w", err)
	}

	headersBuf := bytes.NewBuffer(nil)
	_, err := f.encoder(headersBuf, t.Headers)
	if err != nil {
		return nil, err
	}
	if headersBuf.Len() > math.MaxUint16 {
		return nil, fmt.Errorf("headers too long: %d bytes, maximum is: %d bytes", headersBuf.Len(), math.MaxUint16)
	}
	headersLenAsUint16 := util.SafeIntToUint16(headersBuf.Len())
	// write to the buf 2 bytes indicating header length
	if err := buf.WriteByte(byte(headersLenAsUint16 >> 8)); err != nil {
		return nil, fmt.Errorf("error writing header length high byte: %w", err)
	}
	if err := buf.WriteByte(byte(headersLenAsUint16 & 0xFF)); err != nil {
		return nil, fmt.Errorf("error writing header length low byte: %w", err)
	}
	// Write headers
	if _, err := buf.Write(headersBuf.Bytes()); err != nil {
		return nil, fmt.Errorf("error writing headers: %w", err)
	}
	if _, err := buf.Write(t.Body); err != nil {
		return nil, fmt.Errorf("error writing data: %w", err)
	}

	return buf.Bytes(), nil
}

// Deserialize implements MessageFormat interface
func (f *binaryMessageFormat) Deserialize(data []byte) (*Task, error) {
	buf := bytes.NewBuffer(data)

	// Read topic length
	typeLen, err := buf.ReadByte()
	if err != nil {
		return nil, errors.New("message too short")
	}

	// Read topic
	topicBytes := make([]byte, typeLen)
	if _, err := io.ReadFull(buf, topicBytes); err != nil {
		return nil, errors.New("message too short for type string")
	}
	taskType := string(topicBytes)

	// Read header length (2 bytes)
	headerLenBytes := make([]byte, 2)
	if _, err := io.ReadFull(buf, headerLenBytes); err != nil {
		return nil, errors.New("message too short for header length")
	}
	headersLen := int(headerLenBytes[0])<<8 | int(headerLenBytes[1])

	// Read headers
	headerBytes := make([]byte, headersLen)
	if _, err := io.ReadFull(buf, headerBytes); err != nil {
		return nil, errors.New("message too short for headers")
	}
	var headers map[string]string
	if err := f.decoder(bytes.NewReader(headerBytes), &headers); err != nil {
		return nil, fmt.Errorf("failed to decode headers: %w", err)
	}

	return &Task{
		Type:    taskType,
		Headers: headers,
		Body:    buf.Bytes(),
	}, nil
}
