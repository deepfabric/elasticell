package gcnotifier

import (
	"io/ioutil"
	"runtime"
	"testing"
	"time"
)

func TestAfterGC(t *testing.T) {
	doneCh := make(chan struct{})

	go func() {
		M := &runtime.MemStats{}
		NumGC := uint32(0)
		gcn := New()
		for range gcn.AfterGC() {
			runtime.ReadMemStats(M)
			NumGC += 1
			if NumGC != M.NumGC {
				t.Fatal("Skipped a GC notification")
			}
			if NumGC >= 500 {
				gcn.Close()
			}
		}
		doneCh <- struct{}{}
	}()

	for {
		select {
		case <-time.After(1 * time.Millisecond):
			b := make([]byte, 1<<20)
			b[0] = 1
		case <-doneCh:
			return
		}
	}
}

func TestDoubleClose(t *testing.T) {
	gcn := New()
	gcn.Close()
	gcn.Close() // no-op
}

func TestAutoclose(t *testing.T) {
	count := 10000
	done := make(chan struct{})
	go func() {
		for i := 0; i < count; i++ {
			gcn := New().AfterGC()
			go func() {
				for range gcn {
				}
				// to reach here autoclose() must have been called
				done <- struct{}{}
			}()
		}
	}()
	for i := 0; i < count; {
		select {
		case <-done:
			i++
		default:
			runtime.GC() // required to quickly trigger autoclose()
		}
	}
}

// Example implements a simple time-based buffering io.Writer: data sent over
// dataCh is buffered for up to 100ms, then flushed out in a single call to
// out.Write and the buffer is reused. If GC runs, the buffer is flushed and
// then discarded so that it can be collected during the next GC run. The
// example is necessarily simplistic, a real implementation would be more
// refined (e.g. on GC flush or resize the buffer based on a threshold,
// perform asynchronous flushes, properly signal completions and propagate
// errors, adaptively preallocate the buffer based on the previous capacity,
// etc.)
func Example() {
	dataCh := make(chan []byte)
	doneCh := make(chan struct{})

	out := ioutil.Discard

	go func() {
		var buf []byte
		var tick <-chan time.Time
		gcn := New()

		for {
			select {
			case data := <-dataCh:
				if tick == nil {
					tick = time.After(100 * time.Millisecond)
				}
				// received data to write to the buffer
				buf = append(buf, data...)
			case <-tick:
				// time to flush the buffer (but reuse it for the next writes)
				if len(buf) > 0 {
					out.Write(buf)
					buf = buf[:0]
				}
				tick = nil
			case <-gcn.AfterGC():
				// GC just ran: flush and then drop the buffer
				if len(buf) > 0 {
					out.Write(buf)
				}
				buf = nil
				tick = nil
			case <-doneCh:
				// close the writer: flush the buffer and return
				if len(buf) > 0 {
					out.Write(buf)
				}
				return
			}
		}
	}()

	for i := 0; i < 1<<20; i++ {
		dataCh <- make([]byte, 1024)
	}
	doneCh <- struct{}{}
}
