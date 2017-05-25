package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// Options nemo instance option
type Options struct {
	c *C.nemo_options_t
}

// NEMO instance handle
type NEMO struct {
	c      *C.nemo_t
	dbPath string
	opts   *Options
}

// NewDefaultOptions create default option
func NewDefaultOptions() *Options {
	opts := C.nemo_CreateOption()
	return &Options{c: opts}
}

// OpenNemo return a nemo handle
func OpenNemo(opts *Options, path string) *NEMO {
	var (
		cPath = C.CString(path)
	)
	defer C.free(unsafe.Pointer(cPath))
	nemo := C.nemo_Create(cPath, opts.c)
	return &NEMO{
		c:      nemo,
		dbPath: path,
		opts:   opts,
	}
}

// Close nemo instance
func (nemo *NEMO) Close() {
	C.nemo_free(nemo.c)
}

func (nemo *NEMO) Compact(dbType DBType, sync bool) error {
	var cErr *C.char
	C.nemo_Compact(nemo.c, C.int(dbType), C.bool(sync), &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) RunBGTask() error {
	var cErr *C.char
	C.nemo_RunBGTask(nemo.c, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) GetCurrentTaskType() *string {
	cTaskType := C.nemo_GetCurrentTaskType(nemo.c)
	res := C.GoString(cTaskType)
	C.free(unsafe.Pointer(cTaskType))
	return &res
}
