package cache

import (
	"fmt"
	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/bufpool"
	"github.com/vmihailenco/msgpack/v5"
)

const compressionThreshold = 64

const (
	noCompression = 0x0
	s2Compression = 0x1
)

var _ Marshaller = msgpackMarshaller{}

type msgpackMarshaller struct {
	bufpool bufpool.Pool
}

func (m msgpackMarshaller) Marshal(value interface{}) ([]byte, error) {
	buf := m.bufpool.Get()
	defer m.bufpool.Put(buf)

	enc := msgpack.GetEncoder()
	enc.Reset(buf)
	enc.UseCompactInts(true)
	enc.UseJSONTag(true)

	err := enc.Encode(value)

	msgpack.PutEncoder(enc)

	if err != nil {
		return nil, err
	}

	return compress(buf.Bytes()), nil
}

func (m msgpackMarshaller) Unmarshal(b []byte, value interface{}) error {
	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]

		n, err := s2.DecodedLen(b)
		if err != nil {
			return err
		}

		buf := bufpool.Get(n)
		defer bufpool.Put(buf)

		b, err = s2.Decode(buf.Bytes(), b)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown compression method: %x", c)
	}

	return unmarshal(b, value)
}

func compress(data []byte) []byte {
	if len(data) < compressionThreshold {
		n := len(data) + 1
		b := make([]byte, n, n+timeLen)
		copy(b, data)
		b[len(b)-1] = noCompression
		return b
	}

	n := s2.MaxEncodedLen(len(data)) + 1
	b := make([]byte, n, n+timeLen)
	b = s2.Encode(b, data)
	b = append(b, s2Compression)
	return b
}

// modified from github.com/vmihailenco/msgpack/v5@v5.0.0-alpha.2/decode.go:Unmarshal
//
// add dec.UseJSONTag(true)
func unmarshal(data []byte, v interface{}) error {
	dec := msgpack.GetDecoder()

	dec.ResetBytes(data)
	dec.UseJSONTag(true)
	err := dec.Decode(v)

	msgpack.PutDecoder(dec)

	return err
}
