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

package pdapi

const (
	// CodeSuccess success code
	CodeSuccess = 0
	// CodeError error code
	CodeError = 1
)

// Result is the return value of api server
type Result struct {
	Code  int         `json:"code"`
	Error string      `json:"error, omitempty"`
	Value interface{} `json:"value, omitempty"`
}
