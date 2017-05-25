package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

func (nemo *NEMO) PfAdd(key []byte, vals [][]byte) (bool, error) {
	var cErr *C.char
	var cUpdate C.bool
	l := len(vals)

	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)

	for i, val := range vals {
		cvallist[i] = goBytedup2char(val)
		cvallen[i] = C.size_t(len(val))
	}

	C.nemo_PfAdd(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		&cUpdate,
		&cErr,
	)

	for _, val := range cvallist {
		C.free(unsafe.Pointer(val))
	}

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return false, err
	}
	return bool(cUpdate), nil
}

func (nemo *NEMO) PfCount(keys [][]byte) (int, error) {
	var cErr *C.char
	var cRes C.int
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_PfCount(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&cRes,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int(cRes), nil
}

func (nemo *NEMO) PfMerge(keys [][]byte) (int64, error) {
	var cErr *C.char
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_PfMerge(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return 0, nil
}
