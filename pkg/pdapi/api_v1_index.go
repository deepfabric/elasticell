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

type indexHandler struct {
	service Service
	rd      *render.Render
}

func initAPIForIndex(router *mux.Router, service Service, rd *render.Render) {
	handler := newIndexHandler(service, rd)

	router.HandleFunc("/api/v1/indices/{id}", handler.get).Methods("GET")
	router.HandleFunc("/api/v1/indices/{id}", handler.delete).Methods("DELETE")
	router.HandleFunc("/api/v1/indices", handler.create).Methods("POST")
	router.HandleFunc("/api/v1/indices", handler.list).Methods("GET")
}

func newIndexHandler(service Service, rd *render.Render) *indexHandler {
	return &indexHandler{
		service: service,
		rd:      rd,
	}
}

func (h *indexHandler) get(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	vars := mux.Vars(r)
	idxStr := vars["id"]
	idx, err := h.service.GetIndex(idxStr)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	}

	result.Value = idx

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *indexHandler) delete(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	vars := mux.Vars(r)
	idxStr := vars["id"]
	err := h.service.DeleteIndex(idxStr)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *indexHandler) create(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	idxDef, err := readIndexDef(r.Body)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		err = h.service.CreateIndex(idxDef)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *indexHandler) list(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	indices, err := h.service.ListIndex()
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	}

	result.Value = indices

	h.rd.JSON(w, http.StatusOK, result)
}
