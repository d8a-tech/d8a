package encoding

import (
	"encoding/gob"
	"io"
)

// GobDecoder decodes gob data from a reader into a value
func GobDecoder(r io.Reader, v any) error {
	return gob.NewDecoder(r).Decode(v)
}

// GobEncoder encodes a value into gob and writes it to a writer
func GobEncoder(w io.Writer, v any) (int, error) {
	bcw := &byteCountWriter{w: w}
	err := gob.NewEncoder(bcw).Encode(v)
	return bcw.count, err
}
