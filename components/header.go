package components

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

type Header struct {
	Checksum  uint32
	Timestamp uint32
	KeySize   uint32
	ValueSize uint32
}

func NewHeader(key string, value []byte) *Header {
	return &Header{
		Checksum:  crc32.ChecksumIEEE([]byte(key + string(value))),
		Timestamp: uint32(time.Now().UnixMilli()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
}

func (h *Header) Encode(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, h)
}

func (h *Header) Decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, h)
}
