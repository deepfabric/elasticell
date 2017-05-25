package gonemo

// #cgo CXXFLAGS: -std=c++11
// #cgo LDFLAGS: -lstdc++
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all -lrt
import "C"

import (
        _ "github.com/deepfabric/c-nemo"
)
