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

package util

import (
	"runtime"

	"github.com/deepfabric/elasticell/pkg/log"
)

// Marashal marashal interface
type Marashal interface {
	Size() int
	Marshal() ([]byte, error)
	MarshalTo(data []byte) (int, error)
}

// Unmarshal unmarashal interface
type Unmarshal interface {
	Unmarshal([]byte) error
}

// MustUnmarshal if unmarshal failed, will panic
func MustUnmarshal(target Unmarshal, data []byte) {
	err := target.Unmarshal(data)
	if err != nil {
		buf := make([]byte, 2048)
		runtime.Stack(buf, true)
		log.Fatalf("unmarshal failed, data=<%v>, target=<%+v> string=<%s> errors:\n %+v \n %s",
			data,
			target,
			data,
			err,
			buf)
	}
}

// MustMarshal if marsh failed, will panic
func MustMarshal(target Marashal) []byte {
	data, err := target.Marshal()
	if err != nil {
		log.Fatalf("marshal failed, errors:\n %+v",
			err)
	}

	return data
}

// MustMarshalTo if marsh failed, will panic
func MustMarshalTo(target Marashal, data []byte) int {
	n, err := target.MarshalTo(data)
	if err != nil {
		log.Fatalf("marshal failed, errors:\n %+v",
			err)
	}

	return n
}
