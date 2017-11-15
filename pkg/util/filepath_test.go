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
	"testing"
)

func TestReplaceFpExt(t *testing.T) {
	fps := []string{"./pd1.log", "/tmp/a", "b.txt", ""}
	newFps := []string{"./pd1-etcd.log", "/tmp/a-etcd.log", "b-etcd.log", "-etcd.log"}
	for i, fp := range fps {
		newFp := ReplaceFpExt(fp, "-etcd.log")
		if newFp != newFps[i] {
			t.Fatalf("newFp is incorrect, have %v, want %v. fp: %v\n", newFp, newFps[i], fp)
		}
	}
}
