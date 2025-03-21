package bitcask

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"rodrigocitadin/bitcask/components"
	"time"
)

func (b *Bitcask) Put(key string, value []byte) error {
	b.Lock()
	defer b.Unlock()

	header := components.Header{
		Checksum:  crc32.ChecksumIEEE(value),
		Timestamp: uint32(time.Now().UnixMilli()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}

	buf := b.BufPool.Get().(*bytes.Buffer)
	defer b.BufPool.Put(buf)
	defer buf.Reset()

	header.Encode(buf)
	buf.WriteString(key)
	buf.Write(value)

	offset, err := b.Datafile.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("Error writing data to file: %v", err)
	}

	b.Keydir[key] = components.Meta{
		Timestamp:  int(header.Timestamp),
		RecordSize: buf.Len(),
		RecordPos:  offset + buf.Len(),
		FileID:     b.Datafile.ID,
	}

	return nil
}

func (b *Bitcask) Get(key string) (*components.Record, error) {
	meta, ok := b.Keydir[key]
	if !ok {
		return nil, fmt.Errorf("Key not found: %v", key)
	}
	var header components.Header

	reader := b.Datafile
	if meta.FileID != reader.ID {
		reader, ok = b.Stale[meta.FileID]
		if !ok {
			return nil, fmt.Errorf("Error looking up for the db file or the given id: %d", meta.FileID)
		}
	}

	data, err := reader.Read(meta.RecordPos, meta.RecordSize)
	if err != nil {
		return nil, fmt.Errorf("Error reading data from file: %v", err)
	}

	if err := header.Decode(data); err != nil {
		return nil, fmt.Errorf("Error decoding header: %v", err)
	}

	valPos := meta.RecordSize - int(header.ValueSize)
	value := data[valPos:]

	return &components.Record{
		Header: header,
		Key:    key,
		Value:  value,
	}, nil
}
