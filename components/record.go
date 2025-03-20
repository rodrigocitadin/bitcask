package components

import "hash/crc32"

type Record struct {
	Header Header
	Key    string
	Value  []byte
}

func NewRecord(key string, value []byte) *Record {
	hd := NewHeader(key, value)
	return &Record{
		Header: *hd,
		Key:    key,
		Value:  value,
	}
}

func (r *Record) IsValidChecksum() bool {
	return crc32.ChecksumIEEE([]byte(r.Key+string(r.Value))) == r.Header.Checksum
}
