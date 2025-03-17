package bitcask

import (
	"os"
	"sync"
)

type Entry struct {
	Offset int64
	Length int
}

type Bitcask struct {
	file  *os.File
	index map[string]Entry
	mu    sync.RWMutex
}

func Open(path string) (*Bitcask, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &Bitcask{
		file:  file,
		index: make(map[string]Entry),
	}, nil
}

func (bc *Bitcask) Close() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.file.Close()
}
