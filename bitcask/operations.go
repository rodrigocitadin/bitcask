package bitcask

import (
	"encoding/binary"
	"io"
)

func (bc *Bitcask) Put(key string, value []byte) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	offset, err := bc.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	keySize := len(key)
	valueSize := len(value)

	buf := make([]byte, 8+keySize+8+valueSize)
	binary.LittleEndian.PutUint64(buf[:8], uint64(keySize))
	copy(buf[8:8+keySize], key)
	binary.LittleEndian.PutUint64(buf[8+keySize:16+keySize], uint64(valueSize))
	copy(buf[16+keySize:], value)

	_, err = bc.file.Write(buf)
	if err != nil {
		return err
	}

	bc.index[key] = Entry{Offset: offset, Length: len(buf)}
	return nil
}
