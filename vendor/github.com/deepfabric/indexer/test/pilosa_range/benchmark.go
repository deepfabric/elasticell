/*
https://www.pilosa.com/blog/range-encoded-bitmaps/

测试 pilosa range-encoded-bitmaps
*/

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pilosa/pilosa"
)

var (
	pprof   = flag.String("addr-pprof", "", "pprof http server address")
	logFile = flag.String("log-file", "", "pilosa fragment log file")
)

func fragmentPath(sliceID int) string {
	return filepath.Join("/tmp/pilosa_range/fragments", strconv.FormatInt(int64(sliceID), 10))
}

// GetLogWriter opens a file for logging, or a default io.Writer (such as stderr) for an empty path.
func GetLogWriter(path string, defaultWriter io.Writer) (io.Writer, error) {
	// This is split out so it can be used in NewServeCmd as well as SetupServer
	if path == "" {
		return defaultWriter, nil
	} else {
		logFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		return logFile, nil
	}
}

func main() {
	flag.Parse()
	N := 1000000
	Q := 17000
	R := 500
	S := 1000

	if "" != *pprof {
		log.Printf("bootstrap: start pprof at: %s", *pprof)
		go func() {
			log.Fatalf("bootstrap: start pprof failed, errors:\n%+v",
				http.ListenAndServe(*pprof, nil))
		}()
	}

	// record time
	t0 := time.Now()

	fragments := make(map[int]*pilosa.Fragment) //map slice to Fragment
	var sliceID int
	var frag *pilosa.Fragment
	var ok bool
	var err error
	var logOutput io.Writer
	if logOutput, err = GetLogWriter(*logFile, ioutil.Discard); err != nil {
		log.Fatal(err)
	}

	if err = os.RemoveAll("/tmp/pilosa_range/fragments"); err != nil {
		log.Fatal(err)
	}
	if err = os.MkdirAll("/tmp/pilosa_range/fragments", 0700); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < N; i++ {
		sliceID = i / pilosa.SliceWidth
		if frag, ok = fragments[sliceID]; !ok {
			fp := fragmentPath(sliceID)
			frag = pilosa.NewFragment(fp, "index", "frame", pilosa.ViewStandard, uint64(sliceID))
			frag.MaxOpN = frag.MaxOpN * 100
			frag.CacheType = pilosa.CacheTypeNone
			frag.LogOutput = logOutput
			if err = frag.Open(); err != nil {
				log.Fatal(err)
				return
			}
			fragments[sliceID] = frag
		}
		_, err = frag.SetFieldValue(uint64(i), uint(32), uint64(i))
		if err != nil {
			log.Fatalf("frag.SetFieldValue failed, i=%v, err: %+v", i, err)
		}
	}

	// record time, and calculate performance
	t1 := time.Now()
	log.Printf("duration %v", t1.Sub(t0))
	log.Printf("insertion speed %f docs/s", float64(N)/t1.Sub(t0).Seconds())
	fmt.Printf("duration %v\n", t1.Sub(t0))
	fmt.Printf("insertion speed %f docs/s\n", float64(N)/t1.Sub(t0).Seconds())

	var bs *pilosa.Bitmap
	vals := make([]uint64, 0, 1000)
	for i := N - 1; i >= N-Q; i-- {
		vals = vals[:0]
		sliceID = i / pilosa.SliceWidth
		if frag, ok = fragments[sliceID]; !ok {
			log.Fatalf("frag %v doesn't exist", sliceID)
		}
		bs, err = frag.FieldRangeBetween(uint(32), uint64(i-R), uint64(i+R))
		if err != nil {
			log.Fatalf("frag.FieldRangeBetween failed, i=%v, err: %+v", i, err)
		}
		var val uint64
		var exists bool
		for _, docID := range bs.Bits() {
			if val, exists, err = frag.FieldValue(docID, uint(32)); err != nil {
				log.Fatalf("frag.FieldValue failed, i=%v, err: %+v", i, err)
			}
			if !exists {
				log.Fatalf("document %v doesn't exist", docID)
			}
			vals = append(vals, val)
		}
		//log.Printf("vals %v\n", vals)
	}

	// record time, and calculate performance
	t2 := time.Now()
	log.Printf("duration %v", t2.Sub(t1))
	log.Printf("query speed %f queries/s", float64(Q)/t2.Sub(t1).Seconds())
	log.Printf("bs: %v", bs.Bits())

	_, _ = logOutput.Write([]byte("begin snapshot loop......\n"))
	for i := 0; i < S; i++ {
		for _, frag = range fragments {
			if err = frag.Snapshot(); err != nil {
				log.Fatal(err)
			}
		}
	}

	// record time, and calculate performance
	t3 := time.Now()
	log.Printf("duration %v", t3.Sub(t2))
	log.Printf("snapshot speed %f fragment-snapshots/s", float64(S)/t3.Sub(t2).Seconds())
}
