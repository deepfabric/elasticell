package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// Get a key return a value
func (nemo *NEMO) Get0(key []byte) (*Slice, error) {
	var v C.nemoStr
	var cErr *C.char

	C.nemo_Get0(nemo.c, goByte2char(key), C.size_t(len(key)), &v, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	return NewSlice(&v), nil
}

func (nemo *NEMO) Get(key []byte) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_Get(nemo.c, goByte2char(key), C.size_t(len(key)), &cVal, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.free(unsafe.Pointer(cVal))
	return val, nil
}

// Set a key by value
func (nemo *NEMO) Set(key []byte, value []byte, ttl int) error {
	var (
		cErr *C.char
	)
	C.nemo_Set(nemo.c, goByte2char(key), C.size_t(len(key)), goByte2char(value), C.size_t(len(value)), C.int32_t(ttl), &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) MGet(keys [][]byte) ([][]byte, []error) {
	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)
	cErrs := make([]*C.char, l)
	errs := make([]error, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}
	C.nemo_MGet(nemo.c, C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		(**C.char)(unsafe.Pointer(&cErrs[0])),
	)
	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	for i, cerr := range cErrs {
		if cerr == nil {
			errs[i] = nil
		} else {
			errs[i] = errors.New(C.GoString(cerr))
			C.free(unsafe.Pointer(cerr))
		}
	}
	return cSlice2MultiByte(l, cvallist, cvallen), errs
}

func (nemo *NEMO) MSet(keys [][]byte, vals [][]byte) error {
	var cErr *C.char
	l := len(keys)
	if len(vals) != l {
		return errors.New("key len != val len")
	}
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}
	for i, val := range vals {
		cvallist[i] = goBytedup2char(val)
		cvallen[i] = C.size_t(len(val))
	}

	C.nemo_MSet(nemo.c, C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}
	for _, val := range cvallist {
		C.free(unsafe.Pointer(val))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) Keys(pattern []byte) ([][]byte, error) {
	var cPattern *C.char = goByte2char(pattern)
	var cPatternlen C.size_t = C.size_t(len(pattern))
	var n C.int
	var keylist **C.char
	var keylistlen *C.size_t
	var cErr *C.char
	C.nemo_Keys(nemo.c, cPattern, cPatternlen, &n, &keylist, &keylistlen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	} else {
		return cstr2GoMultiByte(int(n), keylist, keylistlen), nil
	}
}

func (nemo *NEMO) Incrby(key []byte, by int64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_Incrby(nemo.c, goByte2char(key), C.size_t(len(key)), C.int64_t(by), &cRes, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	Res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return Res, nil
}

func (nemo *NEMO) Decrby(key []byte, by int64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_Decrby(nemo.c, goByte2char(key), C.size_t(len(key)), C.int64_t(by), &cRes, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	Res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return Res, nil
}

func (nemo *NEMO) IncrbyFloat(key []byte, by float64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_Incrbyfloat(nemo.c, goByte2char(key), C.size_t(len(key)), C.double(by), &cRes, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	Res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return Res, nil
}

func (nemo *NEMO) GetSet(key []byte, value []byte, ttl int) ([]byte, error) {
	var (
		cErr       *C.char
		cOldVal    *C.char
		cOldValLen C.size_t
	)
	C.nemo_GetSet(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cOldVal, &cOldValLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	OldVal := C.GoBytes(unsafe.Pointer(cOldVal), C.int(cOldValLen))
	C.free(unsafe.Pointer(cOldVal))
	return OldVal, nil
}

func (nemo *NEMO) Append(key []byte, value []byte) (int64, error) {
	var (
		cErr    *C.char
		cNewLen C.int64_t
	)
	C.nemo_Append(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cNewLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cNewLen), nil
}

func (nemo *NEMO) Setnx(key []byte, value []byte, ttl int32) (int64, error) {
	var (
		cErr *C.char
		cRet C.int64_t
	)
	C.nemo_Setnx(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cRet, C.int32_t(ttl), &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRet), nil
}

func (nemo *NEMO) Setxx(key []byte, value []byte, ttl int32) (int64, error) {
	var (
		cErr *C.char
		cRet C.int64_t
	)
	C.nemo_Setxx(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cRet, C.int32_t(ttl), &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRet), nil
}

//	nemo_Getrange
//	nemo_Setrange

func (nemo *NEMO) StrLen(key []byte) (int64, error) {
	var cLen C.int64_t
	var cErr *C.char

	C.nemo_Strlen(nemo.c, goByte2char(key), C.size_t(len(key)), &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cLen), nil
}
