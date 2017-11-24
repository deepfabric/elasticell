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
	"sync"

	"golang.org/x/net/context"
)

// Limiter limiter implemention by token
type Limiter struct {
	sync.RWMutex

	max    uint64
	tokens uint64

	cond *sync.Cond
}

// NewLimiter return a limiter with max
func NewLimiter(max uint64) *Limiter {
	return &Limiter{
		max:    max,
		tokens: 0,
		cond:   sync.NewCond(&sync.Mutex{}),
	}
}

// Wait wait until get the token
func (l *Limiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.getToken() {
		return nil
	}

	l.cond.L.Lock()
	for !l.getToken() {
		l.cond.Wait()
	}
	l.cond.L.Unlock()
	return nil
}

// Release release token
func (l *Limiter) Release() {
	l.Lock()
	l.tokens--
	l.Unlock()
	l.cond.Signal()
}

func (l *Limiter) getToken() bool {
	l.Lock()
	succ := l.tokens < l.max
	if succ {
		l.tokens++
	}
	l.Unlock()

	return succ
}
