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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
)

type storeHandler struct {
	service Service
	rd      *render.Render
}

func initAPIForStore(router *mux.Router, service Service, rd *render.Render) {
	handler := newStoreHandler(service, rd)

	router.HandleFunc("/api/v1/stores/{id}/cells", handler.cells).Methods("GET")
	router.HandleFunc("/api/v1/stores/{id}", handler.get).Methods("GET")
	router.HandleFunc("/api/v1/stores/{id}", handler.delete).Methods("DELETE")
	router.HandleFunc("/api/v1/stores", handler.list).Methods("GET")
	router.HandleFunc("/api/v1/stores/log", handler.setLogLevel).Methods("PUT")
}

func newStoreHandler(service Service, rd *render.Render) *storeHandler {
	return &storeHandler{
		service: service,
		rd:      rd,
	}
}

func (h *storeHandler) get(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		store, err := h.service.GetStore(storeID)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}

		result.Value = store
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *storeHandler) cells(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		cells, err := h.service.ListCellInStore(storeID)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}

		result.Value = cells
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *storeHandler) delete(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		_, force := r.URL.Query()["force"]
		err = h.service.DeleteStore(storeID, force)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *storeHandler) list(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	stores, err := h.service.ListStore()
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	}

	result.Value = stores

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *storeHandler) setLogLevel(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	set, err := readSetLogLevel(r.Body)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		err := h.service.SetStoreLogLevel(set)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}
	}

	h.rd.JSON(w, http.StatusOK, result)
}
