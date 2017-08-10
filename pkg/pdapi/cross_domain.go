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

import (
	"net/http"
	"strings"
)

const (
	headerAccess        = "Access-Control-Allow-Origin"
	headerAccessMethods = "Access-Control-Allow-Methods"
	headerAccessHeaders = "Access-Control-Allow-Headers"
	headerAccessValue   = "*"
)

type cross struct {
}

func newCross() *cross {
	return &cross{}
}

func (c *cross) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	options(w, r)

	if strings.ToUpper(r.Method) == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	next(w, r)
}

func options(w http.ResponseWriter, r *http.Request) {
	w.Header().Del(headerAccess)
	w.Header().Set(headerAccess, headerAccessValue)
	w.Header().Set(headerAccessMethods, "OPTIONS, GET, HEAD, POST, PUT, DELETE")
	w.Header().Set(headerAccessHeaders, "Content-Type")
}
