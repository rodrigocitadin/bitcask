package components

import (
	"hash/crc32"
)

type Record struct {
	Header Header
	Key    string
	Value  []byte
}

func (r *Record) IsValidChecksum() bool {
	return crc32.ChecksumIEEE(r.Value) == r.Header.Checksum
}
