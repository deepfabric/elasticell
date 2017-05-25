package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

func (nemo *NEMO) SAdd(key []byte, members ...[]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(members)

	cmemberlist := make([]*C.char, l)
	cmemberlen := make([]C.size_t, l)

	for i, member := range members {
		cmemberlist[i] = goBytedup2char(member)
		cmemberlen[i] = C.size_t(len(member))
	}

	C.nemo_SMAdd(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&cmemberlist[0])),
		(*C.size_t)(unsafe.Pointer(&cmemberlen[0])),
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRes), nil
}

func (nemo *NEMO) SRem(key []byte, members ...[]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(members)

	cmemberlist := make([]*C.char, l)
	cmemberlen := make([]C.size_t, l)

	for i, member := range members {
		cmemberlist[i] = goBytedup2char(member)
		cmemberlen[i] = C.size_t(len(member))
	}

	C.nemo_SMRem(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&cmemberlist[0])),
		(*C.size_t)(unsafe.Pointer(&cmemberlen[0])),
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRes), nil
}

func (nemo *NEMO) SCard(key []byte) (int64, error) {
	var cSize C.int64_t
	var cErr *C.char
	C.nemo_SCard(nemo.c, goByte2char(key), C.size_t(len(key)), &cSize, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cSize), nil
}

func (nemo *NEMO) SMembers(key []byte) ([][]byte, error) {
	var n C.int
	var memberlist **C.char
	var memberlistlen *C.size_t
	var cErr *C.char
	C.nemo_SMembers(nemo.c, goByte2char(key), C.size_t(len(key)), &memberlist, &memberlistlen, &n, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	} else {
		return cstr2GoMultiByte(int(n), memberlist, memberlistlen), nil
	}
}

func (nemo *NEMO) nemo_SUnionStore(dest []byte, keys [][]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SUnionStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
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
	return int64(cRes), nil
}

func (nemo *NEMO) nemo_SUnion(keys [][]byte) ([][]byte, error) {
	var n C.int
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char

	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SUnion(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&n,
		&vallist, &vallistlen,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	} else {
		return cstr2GoMultiByte(int(n), vallist, vallistlen), nil
	}
}

func (nemo *NEMO) nemo_SInterStore(dest []byte, keys [][]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SInterStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
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
	return int64(cRes), nil
}

func (nemo *NEMO) nemo_SInter(keys [][]byte) ([][]byte, error) {
	var n C.int
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char

	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SInter(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&n,
		&vallist, &vallistlen,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	} else {
		return cstr2GoMultiByte(int(n), vallist, vallistlen), nil
	}
}

func (nemo *NEMO) nemo_SDiffStore(dest []byte, keys [][]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SDiffStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
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
	return int64(cRes), nil
}

func (nemo *NEMO) nemo_SDiff(keys [][]byte) ([][]byte, error) {
	var n C.int
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char

	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SDiff(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&n,
		&vallist, &vallistlen,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	} else {
		return cstr2GoMultiByte(int(n), vallist, vallistlen), nil
	}
}

func (nemo *NEMO) SIsMember(key []byte, member []byte) (bool, error) {
	var cIfExist C.bool
	var cErr *C.char
	C.nemo_SIsMember(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(member), C.size_t(len(member)),
		&cIfExist, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return false, res
	}
	return bool(cIfExist), nil
}

func (nemo *NEMO) SPop(key []byte) ([]byte, error) {
	var cMember *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_SPop(nemo.c, goByte2char(key), C.size_t(len(key)), &cMember, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	val := C.GoBytes(unsafe.Pointer(cMember), C.int(cLen))
	C.free(unsafe.Pointer(cMember))
	return val, nil
}

//nemo_SRandomMember

//nemo_SMove
