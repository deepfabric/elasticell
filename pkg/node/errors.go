package node

import (
	"errors"
)

var (
	// ErrNodeAndPDNotMatch error for node and pd server are not match
	ErrNodeAndPDNotMatch = errors.New("Node and pd server are not match")
)
