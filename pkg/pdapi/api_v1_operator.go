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

type operatorHandler struct {
	service Service
	rd      *render.Render
}

func initAPIForOperator(router *mux.Router, service Service, rd *render.Render) {
	handler := newOperatorHandlerr(service, rd)

	router.HandleFunc("/api/v1/operators", handler.list).Methods("GET")
}

func newOperatorHandlerr(service Service, rd *render.Render) *operatorHandler {
	return &operatorHandler{
		service: service,
		rd:      rd,
	}
}

func (h *operatorHandler) list(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	opts, err := h.service.GetOperators()
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	}

	result.Value = opts
	h.rd.JSON(w, http.StatusOK, result)
}
