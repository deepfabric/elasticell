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
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

// GZIP compress a path to a gzip file
func GZIP(path string) error {
	file, err := os.Create(fmt.Sprintf("%s.gz", path))
	if err != nil {
		return err
	}
	defer file.Close()

	w, err := gzip.NewWriterLevel(nil, gzip.BestSpeed)
	if err != nil {
		return err
	}
	defer w.Close()

	_, err = io.Copy(w, file)
	if err != nil {
		return err
	}

	return nil
}

// UnGZIP ungip file
func UnGZIP(file string) error {
	return nil
}
