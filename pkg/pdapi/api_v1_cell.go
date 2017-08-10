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

type cellHandler struct {
	service Service
	rd      *render.Render
}

func initAPIForCell(router *mux.Router, service Service, rd *render.Render) {
	handler := newCellHandler(service, rd)

	router.HandleFunc("/api/v1/cells/{id}", handler.get).Methods("GET")
	router.HandleFunc("/api/v1/cells/{id}/operator", handler.operator).Methods("GET")
	router.HandleFunc("/api/v1/cells/{id}/leader", handler.leader).Methods("PUT")
	router.HandleFunc("/api/v1/cells", handler.list).Methods("GET")
}

func newCellHandler(service Service, rd *render.Render) *cellHandler {
	return &cellHandler{
		service: service,
		rd:      rd,
	}
}

func (h *cellHandler) operator(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	vars := mux.Vars(r)
	cellIDStr := vars["id"]
	cellID, err := strconv.ParseUint(cellIDStr, 10, 64)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		operator, err := h.service.GetOperator(cellID)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}

		result.Value = operator
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *cellHandler) leader(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	transfer, err := readTransferLeader(r.Body)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		err := h.service.TransferLeader(transfer)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *cellHandler) get(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	vars := mux.Vars(r)
	cellIDStr := vars["id"]
	cellID, err := strconv.ParseUint(cellIDStr, 10, 64)
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	} else {
		cell, err := h.service.GetCell(cellID)
		if err != nil {
			result.Code = CodeError
			result.Error = err.Error()
		}

		result.Value = cell
	}

	h.rd.JSON(w, http.StatusOK, result)
}

func (h *cellHandler) list(w http.ResponseWriter, r *http.Request) {
	result := &Result{
		Code: CodeSuccess,
	}

	cells, err := h.service.ListCell()
	if err != nil {
		result.Code = CodeError
		result.Error = err.Error()
	}

	result.Value = cells
	h.rd.JSON(w, http.StatusOK, result)
}
