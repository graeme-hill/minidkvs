package minidkvs

import "testing"

func TestMemoryDatabase(t *testing.T) {
	db, err := NewMemoryDatabase()
	if err != nil {
		t.Error("Failed to create database")
	}

	res, err := db.Get("test")
	if err != nil {
		t.Error("Failed to get non-existant value")
	}
	if res.HasValue {
		t.Error("Get should not have found a value")
	}

	err = db.Set("test", []byte{5})
	if err != nil {
		t.Error("Failed to set a new value")
	}

	res, err = db.Get("test")
	if err != nil {
		t.Error("Failed to get a new value")
	}
	if !res.HasValue {
		t.Error("New value not found")
	}
	if len(res.Value) != 1 {
		t.Error("New value has wrong length")
	}
	if res.Value[0] != 5 {
		t.Error("New value has wrong bytes")
	}

	err = db.Delete("test")
	if err != nil {
		t.Error("Failed to delete 'test'")
	}

	res, err = db.Get("test")
	if err != nil {
		t.Error("Failed to get deleted value")
	}
	if res.HasValue {
		t.Error("Deleted value is still there")
	}

	db.Close()
}
