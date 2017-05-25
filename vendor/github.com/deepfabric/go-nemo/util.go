package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"reflect"
	"unsafe"
)

func nemoStr2GoByte(str *C.nemoStr) []byte {
	var value []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(str.len), int(str.len), uintptr(unsafe.Pointer(str.data))
	return value
}

func goByte2char(b []byte) *C.char {
	if len(b) > 0 {
		return (*C.char)(unsafe.Pointer(&b[0]))
	}
	return nil
}

func goBytedup2char(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		cData := C.malloc(C.size_t(len(b)))
		copy((*[1 << 30]byte)(cData)[0:len(b)], b)
		c = (*C.char)(cData)
	}
	return c
}

func cstr2GoMultiByte(n int, cdatalist **C.char, clenlist *C.size_t) [][]byte {
	mb := make([][]byte, n)
	datalist := cStr2Slice(cdatalist, n)
	lenlist := size_t2Slice(clenlist, n)
	for i, _ := range mb {
		mb[i] = C.GoBytes(unsafe.Pointer(datalist[i]), C.int(lenlist[i]))
		C.free(unsafe.Pointer(datalist[i]))
	}
	//fix origin memory leak
	C.free(unsafe.Pointer(cdatalist))
	C.free(unsafe.Pointer(clenlist))
	return mb
	//	if there is no slice, we can must use very complicate type cast int go to manipulate c array such as below:
	//	(* C.size_t)(unsafe.Pointer(（uintptr(unsafe.Pointer(lenlist)) + i * unsafe.Sizeof(*lenlist)）))
}

func size_t2Slice(data *C.size_t, length int) []C.size_t {
	var res []C.size_t
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	sH.Cap, sH.Len, sH.Data = length, length, uintptr(unsafe.Pointer(data))
	return res
}

func cStr2Slice(data **C.char, length int) []*C.char {
	var res []*C.char
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	sH.Cap, sH.Len, sH.Data = length, length, uintptr(unsafe.Pointer(data))
	return res
}

func cSlice2MultiByte(n int, datalist []*C.char, lenlist []C.size_t) [][]byte {
	mb := make([][]byte, n)
	for i, _ := range mb {
		mb[i] = C.GoBytes(unsafe.Pointer(datalist[i]), C.int(lenlist[i]))
		C.free(unsafe.Pointer(datalist[i]))
	}
	return mb
}

func cInt64s2Slice(data *C.int64_t, length int) []C.int64_t {
	var res []C.int64_t
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	sH.Cap, sH.Len, sH.Data = length, length, uintptr(unsafe.Pointer(data))
	return res
}

func cDoubles2Slice(data *C.double, length int) []C.double {
	var res []C.double
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	sH.Cap, sH.Len, sH.Data = length, length, uintptr(unsafe.Pointer(data))
	return res
}

func cUll2Slice(data *C.ulonglong, length int) []C.ulonglong {
	var res []C.ulonglong
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	sH.Cap, sH.Len, sH.Data = length, length, uintptr(unsafe.Pointer(data))
	return res
}
