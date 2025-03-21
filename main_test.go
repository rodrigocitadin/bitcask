package bitcask

import (
	"os"
	"testing"
)

var (
	testDir             = "database_test"
	testKey             = "testing_key"
	testValue           = []byte("testing_value")
	testBitcaskInstance *Bitcask
)

func setupBitcask() *Bitcask {
	bitcask, _ := NewBitcask(testDir)
	return bitcask
}

func TestMain(m *testing.M) {
	os.MkdirAll(testDir, 0755)
	testBitcaskInstance = setupBitcask()

	exitCode := m.Run()
	defer os.Exit(exitCode)

	os.RemoveAll(testDir)
}
