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
