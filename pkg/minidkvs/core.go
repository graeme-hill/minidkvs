package minidkvs

import (
	"time"

	"github.com/google/uuid"
)

// Storage is interface for loading/saving the database to disk.
type Storage interface {
	Get(key string) (*Value, error)
	Set(key string, v *Value) error
	Delete(key string) error
	GetNodeID() (*uuid.UUID, error)
}

// Database is adapter to storage.
type Database struct {
	storage Storage
	nodeID  uuid.UUID
	msgChan chan dbMessage
}

// Value is a wrapper for all values in the database. Stores metadata necessary
// for synchronization.
type Value struct {
	Version    int
	ModifiedBy uuid.UUID
	ModifiedAt int64
	Deleted    bool
	Content    []byte
}

// Delta is a wrapper object for a database delta (ie: a new, updated or
// deleted key/value pair).
type Delta struct {
	Key   string
	Value *Value
}

// GetResult wraps the result of a database Get() operaion. Value should only
// be used if HasValue is true.
type GetResult struct {
	HasValue bool
	Value    []byte
}

// TryGet wraps a GetResult and includes Error obj.
type TryGet struct {
	Result GetResult
	Error  error
}

// NewDatabase is ctor for Database.
func NewDatabase(storage Storage) (*Database, error) {
	nodeID, err := storage.GetNodeID()
	if err != nil {
		return nil, err
	}

	db := &Database{
		storage: storage,
		nodeID:  *nodeID,
		msgChan: make(chan dbMessage),
	}

	go dbMessageLoop(db)

	return db, nil
}

// newValue wraps the given bytes in a Value object including automatically
// setting version and date fields.
func (d *Database) newValue(key string, bytes []byte, deleted bool) (*Value, error) {
	value, err := d.storage.Get(key)
	if err != nil {
		return nil, err
	}

	version := 1
	if value != nil {
		version = value.Version + 1
	}

	result := &Value{
		Version:    version,
		ModifiedBy: d.nodeID,
		ModifiedAt: time.Now().Unix(),
		Deleted:    deleted,
		Content:    bytes,
	}

	return result, nil
}

// handleReceive takes a delta from another peer and decides what to do with it.
func (d *Database) handleReceive(delta *Delta) error {
	existingIsConflictWinner := func(existing, new *Value) bool {
		if existing.ModifiedAt == new.ModifiedAt {
			return existing.ModifiedBy.String() < new.ModifiedBy.String()
		}
		return existing.ModifiedAt > new.ModifiedAt
	}

	existing, err := d.storage.Get(delta.Key)
	if err != nil {
		return err
	}

	if existing == nil || existingIsConflictWinner(existing, delta.Value) {
		return d.storage.Set(delta.Key, delta.Value)
	}

	return nil
}

// ReceiveRemote accepts deltas from other peers.
func (d *Database) ReceiveRemote(delta *Delta) error {
	errorChan := make(chan error)
	recvMsg := dbMessageReceive{delta: delta, errorChan: errorChan}
	d.msgChan <- newReceiveMessage(&recvMsg)
	return <-errorChan
}

// Close breaks database goroutine out of its message loop. The database is no
// longer usable afterward. Close() should always be called when the database
// object is not going to be used again.
func (d *Database) Close() {
	d.msgChan <- newCloseMessage()
}

// Get fetches the given value from the database. Missing keys are NOT errors.
// When the key is missing error result is nil but GetResult.HasValue will be
// false.
func (d *Database) Get(key string) (GetResult, error) {
	getMsg := dbMessageGet{key: key, replyChan: make(chan TryGet)}
	d.msgChan <- newGetMessage(&getMsg)
	try := <-getMsg.replyChan
	return try.Result, try.Error
}

// Set upserts the given key/value pair.
func (d *Database) Set(key string, value []byte) error {
	errorChan := make(chan error)
	m := dbMessageSet{key: key, value: value, errorChan: errorChan}
	d.msgChan <- newSetMessage(&m)
	return <-errorChan
}

// Delete removes the given key/value pair. If the key doesn't exist then it
// does nothing and does not treat as an error.
func (d *Database) Delete(key string) error {
	errorChan := make(chan error)
	m := dbMessageDelete{key: key, errorChan: errorChan}
	d.msgChan <- newDeleteMessage(&m)
	return <-errorChan
}

type dbMessageType int32

const (
	dbMessageTypeReceive dbMessageType = 0
	dbMessageTypeSet     dbMessageType = 1
	dbMessageTypeGet     dbMessageType = 2
	dbMessageTypeDelete  dbMessageType = 3
	dbMessageTypeClose   dbMessageType = 4
)

type dbMessageReceive struct {
	delta     *Delta
	errorChan chan error
}

type dbMessageSet struct {
	key       string
	value     []byte
	errorChan chan error
}

type dbMessageGet struct {
	key       string
	replyChan chan TryGet
}

type dbMessageDelete struct {
	key       string
	errorChan chan error
}

type dbMessage struct {
	msgType    dbMessageType
	receiveMsg *dbMessageReceive
	setMsg     *dbMessageSet
	getMsg     *dbMessageGet
	deleteMsg  *dbMessageDelete
}

func newReceiveMessage(data *dbMessageReceive) dbMessage {
	return dbMessage{
		msgType:    dbMessageTypeReceive,
		receiveMsg: data,
	}
}

func newSetMessage(data *dbMessageSet) dbMessage {
	return dbMessage{
		msgType: dbMessageTypeSet,
		setMsg:  data,
	}
}

func newGetMessage(data *dbMessageGet) dbMessage {
	return dbMessage{
		msgType: dbMessageTypeGet,
		getMsg:  data,
	}
}

func newDeleteMessage(data *dbMessageDelete) dbMessage {
	return dbMessage{
		msgType:   dbMessageTypeDelete,
		deleteMsg: data,
	}
}

func newCloseMessage() dbMessage {
	return dbMessage{
		msgType: dbMessageTypeClose,
	}
}

func dbMessageLoop(db *Database) {
	receive := func(m *dbMessageReceive) {
		m.errorChan <- db.handleReceive(m.delta)
	}

	set := func(m *dbMessageSet) {
		value, err := db.newValue(m.key, m.value, false)
		if err != nil {
			m.errorChan <- err
			return
		}
		m.errorChan <- db.storage.Set(m.key, value)
	}

	get := func(m *dbMessageGet) {
		value, err := db.storage.Get(m.key)
		if err != nil {
			m.replyChan <- TryGet{Error: err}
			return
		}

		if value == nil || value.Deleted {
			res := GetResult{HasValue: false}
			m.replyChan <- TryGet{Result: res, Error: nil}
		} else {
			res := GetResult{HasValue: true, Value: value.Content}
			m.replyChan <- TryGet{Result: res, Error: nil}
		}
	}

	delete := func(m *dbMessageDelete) {
		value, err := db.newValue(m.key, nil, true)
		if err != nil {
			m.errorChan <- err
			return
		}
		m.errorChan <- db.storage.Set(m.key, value)
	}

	for {
		msg := <-db.msgChan

		switch msg.msgType {
		case dbMessageTypeReceive:
			receive(msg.receiveMsg)
		case dbMessageTypeSet:
			set(msg.setMsg)
		case dbMessageTypeGet:
			get(msg.getMsg)
		case dbMessageTypeDelete:
			delete(msg.deleteMsg)
		default: // Anything else treated as close.
			break
		}
	}
}
