package bitcask

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"rodrigocitadin/bitcask/components"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Bitcask struct {
	sync.Mutex

	BufPool  sync.Pool
	Keydir   components.Keydir
	Datafile *components.Datafile
	Stale    map[int]*components.Datafile
}

func NewBitcask(dir string) (*Bitcask, error) {
	var (
		index = 0
		stale = map[int]*components.Datafile{}
	)

	files, err := getDatafiles(dir)
	if err != nil {
		return nil, fmt.Errorf("Error loading data files: %v", err)
	}

	if len(files) > 0 {
		ids, err := getIDs(files)
		if err != nil {
			return nil, fmt.Errorf("Error parsing ids for existing files: %v", err)
		}

		index = ids[len(ids)-1] + 1
		for _, idx := range ids {
			df, err := components.NewDatafile(dir, idx)
			if err != nil {
				return nil, err
			}

			stale[idx] = df
		}
	}

	df, err := components.NewDatafile(dir, index)
	if err != nil {
		return nil, err
	}

	keydir := make(components.Keydir, 0)
	hintsPath := filepath.Join(dir, "bitcask.hint")
	if exists(hintsPath) {
		if err := keydir.Decode(hintsPath); err != nil {
			return nil, fmt.Errorf("Error populating hashtable from hints file: %v", err)
		}
	}

	bitcask := &Bitcask{
		Keydir:   keydir,
		Datafile: df,
		Stale:    stale,
		BufPool: sync.Pool{New: func() any {
			return bytes.NewBuffer([]byte{})
		}},
	}

	return bitcask, nil
}

func getDatafiles(dir string) ([]string, error) {
	files, err := filepath.Glob(fmt.Sprintf("%s/*.db", dir))
	if err != nil {
		return nil, err
	}

	return files, nil
}

func getIDs(files []string) ([]int, error) {
	ids := make([]int, 0)

	for _, f := range files {
		id, err := strconv.ParseInt((strings.TrimPrefix(strings.TrimSuffix(filepath.Base(f), ".db"), "bitcask_")), 10, 32)
		if err != nil {
			return nil, err
		}
		ids = append(ids, int(id))
	}

	sort.Ints(ids)
	return ids, nil
}

func exists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}
