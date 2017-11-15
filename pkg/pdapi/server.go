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
	"github.com/urfave/negroni"
)

const (
	// APIPrefix url prefix for api
	APIPrefix = "/pd"
)

// NewAPIHandler returns a HTTP handler for API.
func NewAPIHandler(service Service) http.Handler {
	engine := negroni.New()

	engine.Use(negroni.NewRecovery())

	router := mux.NewRouter()
	router.PathPrefix(APIPrefix).Handler(negroni.New(
		newRedirector(service),
		newCross(),
		negroni.Wrap(createRouter(APIPrefix, service)),
	))

	engine.UseHandler(router)
	return engine
}

func createRouter(prefix string, service Service) *mux.Router {
	rd := render.New(render.Options{
		IndentJSON: true,
	})

	router := mux.NewRouter().PathPrefix(prefix).Subrouter()
	initAPIForStore(router, service, rd)
	initAPIForCell(router, service, rd)
	initAPIForSystem(router, service, rd)
	initAPIForOperator(router, service, rd)
	initAPIForIndex(router, service, rd)
	return router
}
