package bitcask

import (
	"bytes"
	"testing"
)

func TestPutNewValue(t *testing.T) {
	if err := testBitcaskInstance.Put(testKey, testValue); err != nil {
		t.Errorf("Put(%v, %v), error: %v", testKey, testValue, err)
	}
}

func TestGetRecordByKey(t *testing.T) {
	testBitcaskInstance.Put(testKey, testValue)

	record, err := testBitcaskInstance.Get(testKey)
	if err != nil {
		t.Errorf("Get(%v), error: %v", testKey, err)
	}

	if record.Key != record.Key {
		t.Errorf("Get(%v), expected: %v | received: %v", testKey, testKey, record.Key)
	}

	if !bytes.Equal(record.Value, testValue) {
		t.Errorf("Get(%v), expected: %v | received: %v", testKey, string(testValue), string(record.Value))
	}

	if !record.IsValidChecksum() {
		t.Errorf("Get(%v), invalid checksum", testKey)
	}
}
