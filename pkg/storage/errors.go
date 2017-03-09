package storage

import (
	"errors"
)

var (
	// ErrKeySize key size is 0
	ErrKeySize = errors.New("invalid key size")
	// ErrValueSize value size is 0
	ErrValueSize = errors.New("invalid value size")
)
