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
	"github.com/deepfabric/elasticell/pkg/log"
)

// Marashal marashal interface
type Marashal interface {
	Marshal() ([]byte, error)
}

// Unmarshal unmarashal interface
type Unmarshal interface {
	Unmarshal([]byte) error
}

// MustUnmarshal if unmarshal failed, will panic
func MustUnmarshal(target Unmarshal, data []byte) {
	err := target.Unmarshal(data)
	if err != nil {
		log.Fatalf("unmarshal failed, data=<%v>, target=<%+v> errors:\n %+v",
			data,
			target,
			err)
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
