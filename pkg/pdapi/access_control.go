package pdapi

import (
	"net/http"
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
	w.Header().Del(headerAccess)
	w.Header().Set(headerAccess, headerAccessValue)
	next(w, r)
}

func options(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(headerAccessMethods, "OPTIONS, GET, HEAD, POST, PUT, DELETE")
	w.Header().Set(headerAccessHeaders, "Content-Type")

	w.WriteHeader(http.StatusOK)
}
