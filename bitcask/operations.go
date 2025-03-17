package bitcask

import (
	"encoding/binary"
	"errors"
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

func (bc *Bitcask) Get(key string) ([]byte, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	entry, exists := bc.index[key]
	if !exists {
		return nil, errors.New("Key not found")
	}

	buf := make([]byte, entry.Length)
	_, err := bc.file.ReadAt(buf, entry.Offset)
	if err != nil {
		return nil, err
	}

	keySize := binary.LittleEndian.Uint64(buf[:8])
	valueSize := binary.LittleEndian.Uint64(buf[8+keySize : 16+keySize])
	value := buf[16+keySize : 16+keySize+valueSize]

	return value, nil
}

func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()

	if _, exists := bc.index[key]; !exists {
		bc.mu.Unlock()
		return errors.New("Key not found")
	}

	bc.mu.Unlock()
	if err := bc.Put(key, []byte{}); err != nil {
		return err
	}

	bc.mu.Lock()
	defer bc.mu.Unlock()
	delete(bc.index, key)

	return nil
}

func (bc *Bitcask) ListKeys() []string {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	keys := make([]string, 0, len(bc.index))
	for key := range bc.index {
		keys = append(keys, key)
	}
	return keys
}
