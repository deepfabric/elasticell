package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

func (nemo *NEMO) BitSet(key []byte, offset int64, on int64) (int64, error) {
	var cRes C.int64_t
	var cErr *C.char

	C.nemo_BitSet(
		nemo.c, goByte2char(key), C.size_t(len(key)),
		C.int64_t(offset), C.int64_t(on),
		&cRes, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

func (nemo *NEMO) BitGet(key []byte, offset int64, on int64) (int64, error) {
	var cRes C.int64_t
	var cErr *C.char

	C.nemo_BitSet(
		nemo.c, goByte2char(key), C.size_t(len(key)),
		C.int64_t(offset), C.int64_t(on),
		&cRes, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

func (nemo *NEMO) BitCount(key []byte) (int64, error) {
	var cRes C.int64_t
	var cErr *C.char

	C.nemo_BitCount(
		nemo.c, goByte2char(key), C.size_t(len(key)),
		&cRes, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

func (nemo *NEMO) BitCountRange(key []byte, start int64, end int64) (int64, error) {
	var cRes C.int64_t
	var cErr *C.char

	C.nemo_BitCountRange(
		nemo.c, goByte2char(key), C.size_t(len(key)),
		C.int64_t(start), C.int64_t(end),
		&cRes, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

func (nemo *NEMO) BitPos(key []byte, bit int64) (int64, error) {
	var cRes C.int64_t
	var cErr *C.char

	C.nemo_BitPos(
		nemo.c, goByte2char(key), C.size_t(len(key)),
		C.int64_t(bit),
		&cRes, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

//nemo_BitPosWithStart
//nemo_BitPosWithStartEnd

func (nemo *NEMO) BitOp(opType BitOpType, dest []byte, srckeys [][]byte) (int64, error) {
	var cErr *C.char
	var resLen C.int64_t
	l := len(srckeys)

	csrckeylist := make([]*C.char, l)
	csrckeylen := make([]C.size_t, l)

	for i, srckey := range srckeys {
		csrckeylist[i] = goBytedup2char(srckey)
		csrckeylen[i] = C.size_t(len(srckey))
	}

	C.nemo_BitOp(nemo.c, C.int(opType),
		goByte2char(dest),
		C.size_t(len(dest)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&csrckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&csrckeylen[0])),
		&resLen,
		&cErr,
	)

	for _, srckey := range csrckeylist {
		C.free(unsafe.Pointer(srckey))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(resLen), nil
}
