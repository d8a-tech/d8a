// Package encoding provides functionality for encoding and decoding data
package encoding

import (
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"io"

	"github.com/fxamacker/cbor/v2"
	"github.com/sirupsen/logrus"
)

// DecoderFunc defines a function type for decoding data from a reader into a value
type DecoderFunc func(r io.Reader, v any) error

// EncoderFunc defines a function type for encoding a value into a writer
type EncoderFunc func(w io.Writer, v any) (int, error)

// ZlibCBORDecoder decodes zlib-compressed CBOR data from a reader into a value
func ZlibCBORDecoder(r io.Reader, v any) error {
	reader, err := zlib.NewReader(r)
	if err != nil {
		return err
	}
	defer func() {
		err = reader.Close()
		if err != nil {
			logrus.Errorf("Error closing zlib reader: %v", err)
		}
	}()

	decompressedData, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	return cbor.Unmarshal(decompressedData, v)
}

// ZlibCBOREncoder encodes a value into zlib-compressed CBOR data and writes it to a writer
func ZlibCBOREncoder(w io.Writer, v any) (int, error) {
	compressor := zlib.NewWriter(w)
	defer func() {
		if err := compressor.Close(); err != nil {
			logrus.Errorf("Error closing zlib writer: %v", err)
		}
	}()

	cborData, err := cbor.Marshal(v)
	if err != nil {
		return 0, err
	}

	n, err := compressor.Write(cborData)
	return n, err
}

// JSONDecoder decodes JSON data from a reader into a value
func JSONDecoder(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}

// JSONEncoder encodes a value into JSON and writes it to a writer
func JSONEncoder(w io.Writer, v any) (int, error) {
	// Create a counting writer to track bytes
	countingWriter := &byteCountWriter{w: w}

	// Use the counting writer with the encoder
	err := json.NewEncoder(countingWriter).Encode(v)

	return countingWriter.count, err
}

// GzipJSONDecoder decodes gzip-compressed JSON data from a reader into a value
func GzipJSONDecoder(r io.Reader, v any) error {
	reader, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer func() {
		err = reader.Close()
		if err != nil {
			logrus.Errorf("Error closing gzip reader: %v", err)
		}
	}()

	return json.NewDecoder(reader).Decode(v)
}

// GzipJSONEncoder encodes a value into gzip-compressed JSON and writes it to a writer
func GzipJSONEncoder(w io.Writer, v any) (int, error) {
	countingWriter := &byteCountWriter{w: w}
	compressor := gzip.NewWriter(countingWriter)

	defer func() {
		if err := compressor.Close(); err != nil {
			logrus.Errorf("Error closing gzip writer: %v", err)
		}
	}()

	err := json.NewEncoder(compressor).Encode(v)
	if err != nil {
		return countingWriter.count, err
	}

	// Close the compressor to flush any remaining data
	if err := compressor.Close(); err != nil {
		logrus.Errorf("Error closing gzip writer: %v", err)
		return countingWriter.count, err
	}

	return countingWriter.count, nil
}

// byteCountWriter keeps track of the number of bytes written
type byteCountWriter struct {
	w     io.Writer
	count int
}

// Write implements io.Writer and counts bytes
func (bcw *byteCountWriter) Write(p []byte) (int, error) {
	n, err := bcw.w.Write(p)
	bcw.count += n
	return n, err
}
