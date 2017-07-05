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
	"time"

	"github.com/fagongzi/goetty"
)

var (
	defaultAccuracy = time.Millisecond * 50
	defaultTW       = goetty.NewTimeoutWheel(goetty.WithTickInterval(defaultAccuracy))
)

// DefaultTimeoutWheel returns default timeout wheel
func DefaultTimeoutWheel() *goetty.TimeoutWheel {
	return defaultTW
}
