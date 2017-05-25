package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

type DBNemo struct {
	c *C.nemo_DBNemo_t
}

func (nemo *NEMO) GetMetaHandle() *DBNemo {
	var hd DBNemo
	hd.c = C.nemo_GetMetaHandle(nemo.c)
	return &hd
}

func (nemo *NEMO) GetRaftHandle() *DBNemo {
	var hd DBNemo
	hd.c = C.nemo_GetRaftHandle(nemo.c)
	return &hd
}

func (nemo *NEMO) BatchWrite(db *DBNemo, wb *WriteBatch) error {
	var cErr *C.char
	C.rocksdb_BatchWrite(nemo.c, db.c, wb.c, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) PutWithHandle(db *DBNemo, key []byte, value []byte) error {
	var cErr *C.char
	C.nemo_PutWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) GetWithHandle(db *DBNemo, key []byte) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char
	C.nemo_GetWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)),
		&cVal, &cLen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.free(unsafe.Pointer(cVal))
	return val, nil
}

func (nemo *NEMO) DeleteWithHandle(db *DBNemo, key []byte) error {
	var cErr *C.char
	C.nemo_DeleteWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)), &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}
