package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

type KIterator struct {
	c *C.nemo_KIterator_t
}
type VolumeIterator struct {
	c *C.nemo_VolumeIterator_t
}

func (nemo *NEMO) KScanWithHandle(db *DBNemo, start []byte, end []byte, use_snapshot bool) *KIterator {
	var kit KIterator
	kit.c = C.nemo_KScanWithHandle(nemo.c, db.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(use_snapshot),
	)
	return &kit
}

func (nemo *NEMO) SeekWithHandle(db *DBNemo, start []byte) ([]byte, []byte, error) {
	var cKey *C.char
	var cKeyLen C.size_t
	var cVal *C.char
	var cValLen C.size_t
	var cErr *C.char
	var nKey []byte
	var nVal []byte

	C.nemo_SeekWithHandle(nemo.c, db.c,
		goByte2char(start), C.size_t(len(start)),
		&cKey, &cKeyLen,
		&cVal, &cValLen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, nil, res
	}
	if cKeyLen != 0 {
		nKey = C.GoBytes(unsafe.Pointer(cKey), C.int(cKeyLen))
		C.free(unsafe.Pointer(cKey))
	} else {
		nKey = nil
	}
	if cValLen != 0 {
		nVal = C.GoBytes(unsafe.Pointer(cVal), C.int(cValLen))
		C.free(unsafe.Pointer(cVal))
	} else {
		nVal = nil
	}

	return nKey, nVal, nil

}

func (it *KIterator) Next() {
	C.KNext(it.c)
}

func (it *KIterator) Valid() bool {
	return bool(C.KValid(it.c))
}

func (it *KIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.Kkey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *KIterator) Value() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.Kvalue(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *KIterator) Free() {
	C.KIteratorFree(it.c)
}

func (nemo *NEMO) NewVolumeIterator(start []byte, end []byte) *VolumeIterator {
	var it VolumeIterator
	it.c = C.createVolumeIterator(nemo.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(false),
	)
	return &it
}

func (it *VolumeIterator) Next() {
	C.VolNext(it.c)
}

func (it *VolumeIterator) Valid() bool {
	return bool(C.VolValid(it.c))
}

func (it *VolumeIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.Volkey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *VolumeIterator) Value() int64 {
	var cRes C.int64_t
	C.Volvalue(it.c, &cRes)
	return int64(cRes)
}

func (it *VolumeIterator) Free() {
	C.VolIteratorFree(it.c)
}
