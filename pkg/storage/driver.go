package storage

// KVPair is a key and value pair
type KVPair struct {
	Key   []byte
	Value []byte
}

// Driver is def storage interface
type Driver interface {
	// global commands.

	// Delete is a redis delete command
	Delete(key []byte) error

	// strings commands. See https://redis.io/commands#string
	// Set is a redis set command
	Set(key []byte, value []byte) error
	// SetNX is a redis setnx command
	SetNX(key []byte, value []byte) (bool, error)
	// SetRange is a redis setrange command
	SetRange(key []byte, value []byte, offset int) (int64, error)
	// MSet is a redis mset command
	MSet(pairs ...[]*KVPair) error
	// MSetNX is a redis msetnx command
	MSetNX(pairs ...[]*KVPair) error
	// Append is a redis append command
	Append(key []byte, value []byte) (int64, error)
	// StrLen is a redis strlen command
	StrLen(key []byte) (int64, error)
	// Get is a redis get command
	Get(keys ...[]byte) ([]byte, error)
	// Decr is a redis decr command
	Decr(key []byte) (int64, error)
	// DecrBy is a redis decrby command
	DecrBy(key []byte, decrement int64) (int64, error)
	// Incr is a redis incr command
	Incr(key []byte) (int64, error)
	// IncrBy is a redis incrby command
	IncrBy(key []byte, decrement int64) (int64, error)
	// GetRange is redis getrange command
	GetRange(key []byte, start int, end int) ([]byte, error)
	// GetSet is redis getset command
	GetSet(key []byte, value []byte) ([]byte, error)
	// MGet is redis mget command
	MGet(keys ...[]byte) ([][]byte, error)
}
