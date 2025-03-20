package components

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Datafile struct {
	sync.RWMutex

	writer *os.File
	reader *os.File
	id     int
	offset int
}

func New(dir string, index int) (*Datafile, error) {
	path := filepath.Join(dir, fmt.Sprintf("bitcask_%d.db", index))

	writer, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file for writing db: %v", err)
	}

	reader, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file for reading db: %v", err)
	}

	stat, err := writer.Stat()
	if err != nil {
		return nil, fmt.Errorf("error fetching file stats: %v", err)
	}

	df := &Datafile{
		writer: writer,
		reader: reader,
		id:     index,
		offset: int(stat.Size()),
	}

	return df, nil
}

func (d *Datafile) Close() error {
	if err := d.writer.Close(); err != nil {
		return err
	}

	if err := d.reader.Close(); err != nil {
		return err
	}

	return nil
}
