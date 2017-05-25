package gonemo

// #include "nemo_c.h"
import "C"

// Slice as go type contains a pointer to c++ string
type Slice struct {
	str   *C.nemoStr
	freed bool
}

// NewSlice return a new Slice object
func NewSlice(cstr *C.nemoStr) *Slice {
	return &Slice{
		str:   cstr,
		freed: false,
	}
}

// Data return a byte slice
func (s *Slice) Data() []byte {
	return nemoStr2GoByte(s.str)
}

// Free c++ string
func (s *Slice) Free() {
	if !s.freed {
		C.nemoStrFree(s.str)
		s.freed = true
	}
}
