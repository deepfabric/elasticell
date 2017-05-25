package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

type snapshot struct {
	c        *C.nemo_Snaptshot_t
	ktype    string
	instance *NEMO
}

type snapshots struct {
	c        *C.nemo_Snaptshot_t
	len      int
	instance *NEMO
}

func (nemo *NEMO) BGSaveGetSnapshot() (*snapshots, error) {
	var count C.int
	var cErr *C.char
	var cSnapShots *C.nemo_Snaptshot_t
	C.nemo_BGSaveGetSnapshot(nemo.c,
		&count,
		&cSnapShots,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		C.free(unsafe.Pointer(cSnapShots))
		return nil, err
	}

	return &snapshots{
		c:        cSnapShots,
		len:      int(count),
		instance: nemo,
	}, nil

}

func (nemo *NEMO) BGSave(ss *snapshots, db_path string) error {
	var cErr *C.char
	C.nemo_BGSave(nemo.c,
		C.int(ss.len),
		ss.c,
		C.CString(db_path),
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return err
	}
	return nil
}

func (nemo *NEMO) BGSaveGetSpecifySnapshot(keytype string) (*snapshot, error) {
	var cErr *C.char
	var cSnapShot *C.nemo_Snaptshot_t

	C.nemo_BGSaveGetSpecifySnapshot(nemo.c,
		C.CString(keytype),
		&cSnapShot,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, err
	}
	return &snapshot{
		instance: nemo,
		c:        cSnapShot,
		ktype:    keytype,
	}, nil
}

func (nemo *NEMO) BGSaveSpecify(ss *snapshot) error {
	var cErr *C.char
	C.nemo_BGSaveSpecify(nemo.c,
		C.CString(ss.ktype),
		ss.c,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return err
	}
	return nil
}

func (nemo *NEMO) SaveOff() error {
	var cErr *C.char
	C.nemo_BGSaveOff(nemo.c, &cErr)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return err
	}
	return nil
}

func (nemo *NEMO) GetKeyNum() ([]uint64, error) {
	var cErr *C.char
	var n C.int
	var cNum *C.ulonglong
	C.nemo_GetKeyNum(nemo.c,
		&n,
		&cNum,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, err
	}

	cNumSlice := cUll2Slice(cNum, int(n))
	res := make([]uint64, int(n))
	for i, _ := range res {
		res[i] = uint64(cNumSlice[i])
	}
	return res, nil
}

func (nemo *NEMO) GetSpecifyKeyNum(keytype string) (uint64, error) {
	var cErr *C.char
	var cNum C.ulonglong
	C.nemo_GetSpecifyKeyNum(nemo.c,
		C.CString(keytype),
		&cNum,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return uint64(cNum), nil
}

func (nemo *NEMO) GetUsage(keytype string) (uint64, error) {
	var cErr *C.char
	var cRes C.ulonglong
	C.nemo_GetUsage(nemo.c,
		C.CString(keytype),
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return uint64(cRes), nil
}

//nemo_GetDBByType

//nemo_CheckMetaSpecify

//nemo_ChecknRecover

//nemo_HChecknRecover

//nemo_LChecknRecover

//nemo_SChecknRecover

//nemo_ZChecknRecover
