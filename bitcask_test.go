package bitcask

import "testing"

func TestNewBitcaskInstance(t *testing.T) {
	_, err := NewBitcask(testDir)
	if err != nil {
		t.Errorf("NewBitcask(%v), error: %v", testDir, err)
	}
}
