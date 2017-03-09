package pd

import (
	"errors"
)

var (
	// ErrClusterIsAlreadyBootstrapped error for cluster is already bootstrapped
	ErrClusterIsAlreadyBootstrapped = errors.New("The cluster is already bootstrapped")
)

const (
	// ZeroID use for check
	ZeroID int64 = 0
)
