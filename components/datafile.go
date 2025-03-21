package components

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Datafile struct {
	sync.RWMutex

	Writer *os.File
	Reader *os.File
	ID     int
	Offset int
}

func NewDatafile(dir string, index int) (*Datafile, error) {
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
		Writer: writer,
		Reader: reader,
		ID:     index,
		Offset: int(stat.Size()),
	}

	return df, nil
}

func (d *Datafile) Sync() error {
	return d.Writer.Sync()
}

func (d *Datafile) Read(pos int, size int) ([]byte, error) {
	start := int64(pos - size)
	record := make([]byte, size)

	n, err := d.Reader.ReadAt(record, start)
	if err != nil {
		return nil, err
	}

	if n != int(size) {
		return nil, fmt.Errorf("error fetching record, invalid size")
	}

	return record, nil
}

func (d *Datafile) Write(data []byte) (int, error) {
	if _, err := d.Writer.Write(data); err != nil {
		return -1, err
	}

	offset := d.Offset
	d.Offset += len(data)

	return offset, nil
}

func (d *Datafile) Close() error {
	if err := d.Writer.Close(); err != nil {
		return err
	}

	if err := d.Reader.Close(); err != nil {
		return err
	}

	return nil
}
