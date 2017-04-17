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
	"errors"
	"sync"
	"time"

	"golang.org/x/net/context"
)

var (
	errUnavailable = errors.New("Stopper is unavailable")
)

var (
	defaultWaitStoppedTimeout = time.Minute
)

type state int

const (
	running  = state(0)
	stopping = state(1)
	stopped  = state(2)
)

var (
	// ErrJobCancelled error job cancelled
	ErrJobCancelled = errors.New("Job cancelled")
)

// JobState is the job state
type JobState int

const (
	// Pending job is wait to running
	Pending = JobState(0)
	// Running job is running
	Running = JobState(1)
	// Cancelling job is cancelling
	Cancelling = JobState(2)
	// Cancelled job is cancelled
	Cancelled = JobState(3)
	// Finished job is complete
	Finished = JobState(4)
	// Failed job is failed when execute
	Failed = JobState(5)
)

// Job is do for something with state
type Job struct {
	sync.RWMutex
	fun    func() error
	state  JobState
	result interface{}
}

func newJob(fun func() error) *Job {
	return &Job{
		fun:   fun,
		state: Pending,
	}
}

// IsComplete return true means the job is complete.
func (job *Job) IsComplete() bool {
	return !job.IsNotComplete()
}

// IsNotComplete return true means the job is not complete.
func (job *Job) IsNotComplete() bool {
	job.RLock()
	yes := job.state == Pending || job.state == Running || job.state == Cancelling
	job.RUnlock()

	return yes
}

// SetResult set result
func (job *Job) SetResult(result interface{}) {
	job.Lock()
	job.result = result
	job.Unlock()
}

// GetResult returns job result
func (job *Job) GetResult() interface{} {
	job.RLock()
	r := job.result
	job.RUnlock()
	return r
}

// Cancel cancel the job
func (job *Job) Cancel() {
	job.Lock()
	if job.state == Pending {
		job.state = Cancelling
	}
	job.Unlock()
}

// IsRunning returns true if job state is Running
func (job *Job) IsRunning() bool {
	return job.isSpecState(Running)
}

// IsPending returns true if job state is Pending
func (job *Job) IsPending() bool {
	return job.isSpecState(Pending)
}

// IsFinished returns true if job state is Finished
func (job *Job) IsFinished() bool {
	return job.isSpecState(Finished)
}

// IsCancelling returns true if job state is Cancelling
func (job *Job) IsCancelling() bool {
	return job.isSpecState(Cancelling)
}

// IsCancelled returns true if job state is Cancelled
func (job *Job) IsCancelled() bool {
	return job.isSpecState(Cancelled)
}

// IsFailed returns true if job state is Failed
func (job *Job) IsFailed() bool {
	return job.isSpecState(Failed)
}

func (job *Job) isSpecState(spec JobState) bool {
	job.RLock()
	yes := job.state == spec
	job.RUnlock()

	return yes
}

func (job *Job) setState(state JobState) {
	job.state = state
	job.Unlock()
}

func (job *Job) getState() JobState {
	job.RLock()
	s := job.state
	job.RUnlock()

	return s
}

// TaskRunner TODO
type TaskRunner struct {
	sync.RWMutex

	stop    sync.WaitGroup
	stopC   chan struct{}
	cancels []context.CancelFunc
	state   state
	queue   chan *Job
}

// NewTaskRunner returns a task runner
func NewTaskRunner(workerCnt int, jobQueueCap uint64) *TaskRunner {
	t := &TaskRunner{
		stopC: make(chan struct{}),
		state: running,
		queue: make(chan *Job, jobQueueCap),
	}

	for index := 0; index < workerCnt; index++ {
		t.AddJobWorker()
	}

	return t
}

// AddJobWorker add a job worker using a goroutine
func (s *TaskRunner) AddJobWorker() {
	s.RunCancelableTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-s.queue:
				job.Lock()

				switch job.state {
				case Pending:
					job.setState(Running)
					err := job.fun()
					job.Lock()
					if err != nil {
						if err == ErrJobCancelled {
							job.setState(Cancelled)
						} else {
							job.setState(Failed)
						}
					} else {
						if job.state == Cancelling {
							job.setState(Cancelled)
						} else {
							job.setState(Finished)
						}
					}
				case Cancelling:
					job.setState(Cancelled)
				}
			}
		}
	})
}

// RunJob run a job
func (s *TaskRunner) RunJob(task func() error) (*Job, error) {
	s.RLock()
	defer s.RUnlock()

	if s.state != running {
		return nil, errUnavailable
	}

	job := newJob(task)
	s.queue <- job
	return job, nil
}

// RunCancelableTask run a task that can be cancelled
// Example:
// err := s.RunCancelableTask(func(ctx context.Context) {
// 	select {
// 	case <-ctx.Done():
// 	// cancelled
// 	case <-time.After(time.Second):
// 		// do something
// 	}
// })
// if err != nil {
// 	// hanle error
// 	return
// }
func (s *TaskRunner) RunCancelableTask(task func(context.Context)) error {
	s.RLock()
	defer s.RUnlock()

	if s.state != running {
		return errUnavailable
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancels = append(s.cancels, cancel)

	s.stop.Add(1)

	go func() {
		defer s.stop.Done()
		task(ctx)
	}()

	return nil
}

// RunTask runs a task in new goroutine
func (s *TaskRunner) RunTask(task func()) error {
	s.RLock()
	defer s.RUnlock()

	if s.state != running {
		return errUnavailable
	}

	s.stop.Add(1)

	go func() {
		defer s.stop.Done()
		task()
	}()

	return nil
}

// Stop stop all task
// RunTask will failure with an error
// Wait complete for the tasks that already in execute
// Cancel the tasks that is not start
func (s *TaskRunner) Stop() error {
	s.Lock()
	defer s.Unlock()

	if s.state == stopping ||
		s.state == stopped {
		return errors.New("stopper is already stoppped")
	}
	s.state = stopping

	for _, cancel := range s.cancels {
		cancel()
	}

	go func() {
		s.stop.Wait()
		s.stopC <- struct{}{}
	}()

	select {
	case <-time.After(defaultWaitStoppedTimeout):
		return errors.New("waitting for task complete timeout")
	case <-s.stopC:
	}

	s.state = stopped
	return nil
}

func (s *TaskRunner) recover() {
	if r := recover(); r != nil {
		panic(r)
	}
}
