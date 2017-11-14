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

package pdserver

import (
	"io"
	"time"

	"github.com/coreos/pkg/capnslog"
)

// RedirectEmbedEtcdLog because of our used embed etcd,
// so we need redirect etcd log to spec.
func RedirectEmbedEtcdLog(w io.Writer) {
	capnslog.SetFormatter(capnslog.NewPrettyFormatter(w, false))
	capnslog.SetGlobalLogLevel(capnslog.DEBUG)
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
