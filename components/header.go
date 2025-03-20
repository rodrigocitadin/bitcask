package components

import (
	"bytes"
	"encoding/binary"
)

type Header struct {
	Checksum  uint32
	Timestamp uint32
	KeySize   uint32
	ValueSize uint32
}

func (h *Header) Encode(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, h)
}

func (h *Header) Decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, h)
}
