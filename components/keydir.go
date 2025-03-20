package components

import (
	"encoding/gob"
	"os"
)

type Keydir map[string]Meta

type Meta struct {
	Timestamp  int
	RecordSize int
	RecordPos  int
	FileID     int
}

func (k *Keydir) Encode(fPath string) error {
	file, err := os.Create(fPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(k)
	if err != nil {
		return err
	}

	return nil
}

func (k *Keydir) Decode(fPath string) error {
	file, err := os.Open(fPath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(k)
	if err != nil {
		return err
	}

	return nil
}
