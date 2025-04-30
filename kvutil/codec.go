package kvutil

import (
	"bytes"
	"encoding/gob"
)

func Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, out interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(out)
}

func Register(value interface{}) {
	gob.Register(value)
}
