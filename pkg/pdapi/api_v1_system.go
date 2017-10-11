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

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
)

type systemHandler struct {
	service Service
	rd      *render.Render
}

func initAPIForSystem(router *mux.Router, service Service, rd *render.Render) {
	handler := newSystemHandler(service, rd)

	router.HandleFunc("/api/v1/system", handler.get).Methods("GET")
	router.HandleFunc("/api/v1/system", handler.create).Methods("POST")
}

func newSystemHandler(service Service, rd *render.Render) *systemHandler {
	return &systemHandler{
		service: service,
		rd:      rd,
	}
}

func (h *systemHandler) get(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	system, err := h.service.GetSystem()
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	}

	result.Value = system

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *systemHandler) create(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	params, err := readInitParams(r.Body)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		err := h.service.InitCluster(params)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}
	}

	h.rd.JSON(w, http.StatusOK, result)
}
