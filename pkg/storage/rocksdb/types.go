package rocksdb

const (
	// KVType kv type
	KVType byte = 0x10
	// ListType list type
	ListType byte = 0x11
	// SetType set type
	SetType byte = 0x12
	// HashType hash type
	HashType byte = 0x13
)

func encodeKVKey(key []byte) []byte {
	ek := make([]byte, len(key)+1)
	ek[0] = KVType
	copy(ek[1:], key)
	return ek
}
