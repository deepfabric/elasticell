// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// +build freebsd openbsd netbsd dragonfly linux

package util

import (
	"encoding/json"
	"os"
	"syscall"

	"github.com/pkg/errors"
)

//FileMmap mmaps the given file.
//https://medium.com/@arpith/adventures-with-mmap-463b33405223
func FileMmap(f *os.File) (data []byte, err error) {
	info, err1 := f.Stat()
	if err1 != nil {
		err = errors.Wrap(err1, "")
		return
	}
	prots := []int{syscall.PROT_WRITE | syscall.PROT_READ, syscall.PROT_READ}
	for _, prot := range prots {
		data, err = syscall.Mmap(int(f.Fd()), 0, int(info.Size()), prot, syscall.MAP_SHARED)
		if err == nil {
			break
		}
	}
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//FileMunmap unmaps the given file.
func FileMunmap(data []byte) (err error) {
	err = syscall.Munmap(data)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//FileUnmarshal unmarshals the given file to object.
func FileUnmarshal(fp string, v interface{}) (err error) {
	var f *os.File
	var data []byte
	if f, err = os.Open(fp); err != nil {
		return
	}
	defer f.Close()
	if data, err = FileMmap(f); err != nil {
		return
	}
	defer FileMunmap(data)
	err = json.Unmarshal(data, v)
	return
}

//FileMarshal marshals the given object to file.
func FileMarshal(fp string, v interface{}) (err error) {
	var f *os.File
	var data []byte
	var count int
	if data, err = json.Marshal(v); err != nil {
		return
	}
	if f, err = os.OpenFile(fp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer f.Close()
	if count, err = f.Write(data); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if count != len(data) {
		err = errors.Errorf("%s partial wirte %d, want %d", fp, count, len(data))
		return
	}
	return
}
