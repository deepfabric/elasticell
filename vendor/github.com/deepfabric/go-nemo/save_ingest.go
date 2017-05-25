package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

func (nemo *NEMO) RawScanSaveRange(path string, start []byte, end []byte, use_snapshot bool) error {
	var cErr *C.char
	cPath := C.CString(path)
	C.nemo_RawScanSaveAll(nemo.c,
		cPath,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(use_snapshot),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) IngestFile(path string) error {
	var cErr *C.char
	cPath := C.CString(path)
	C.nemo_IngestFile(nemo.c, cPath, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}
