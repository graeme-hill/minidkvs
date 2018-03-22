package minidkvs

import "github.com/google/uuid"

// MemoryStorage is a pure-memory implementation of Storage interface. Mainly
// just meant for testing.
type MemoryStorage struct {
	data   map[string]Value
	nodeID uuid.UUID
}

// Get reads from in-memory map.
func (m *MemoryStorage) Get(key string) (*Value, error) {
	val, ok := m.data[key]
	if !ok {
		return nil, nil
	}
	return &val, nil
}

// Set upserts value.
func (m *MemoryStorage) Set(key string, value *Value) error {
	m.data[key] = *value
	return nil
}

// Delete deletes value. Missing key is no-op.
func (m *MemoryStorage) Delete(key string) error {
	delete(m.data, key)
	return nil
}

// GetNodeID returns the unique identifier for this node.
func (m *MemoryStorage) GetNodeID() (*uuid.UUID, error) {
	return &m.nodeID, nil
}

// NewMemoryStorage is ctor for MemoryStorage.
func NewMemoryStorage() (*MemoryStorage, error) {
	nodeID, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	storage := &MemoryStorage{
		data:   make(map[string]Value),
		nodeID: nodeID,
	}

	return storage, nil
}

// NewMemoryDatabase is factory function for database connection using an
// in-memory map.
func NewMemoryDatabase() (*Database, error) {
	storage, err := NewMemoryStorage()
	if err != nil {
		return nil, err
	}
	return NewDatabase(storage)
}
