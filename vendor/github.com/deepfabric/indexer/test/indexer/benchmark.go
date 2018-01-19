package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/deepfabric/indexer"
	"github.com/deepfabric/indexer/cql"
)

var (
	pprof = flag.String("addr-pprof", "", "pprof http server address")
)

func newDocProt1() *cql.DocumentWithIdx {
	return &cql.DocumentWithIdx{
		Document: cql.Document{
			DocID: 0,
			UintProps: []cql.UintProp{
				cql.UintProp{
					Name:   "object",
					ValLen: 8,
					Val:    0,
				},
				cql.UintProp{
					Name:   "price",
					ValLen: 4,
					Val:    0,
				},
				cql.UintProp{
					Name:   "number",
					ValLen: 4,
					Val:    0,
				},
				cql.UintProp{
					Name:   "date",
					ValLen: 8,
					Val:    0,
				},
			},
			StrProps: []cql.StrProp{
				cql.StrProp{
					Name: "description",
					Val:  "",
				},
				cql.StrProp{
					Name: "note",
					Val:  "",
				},
			},
		},
		Index: "orders",
	}
}

func prepareIndexer(numDocs int, docProts []*cql.DocumentWithIdx) (ir *indexer.Indexer, err error) {
	//create indexer
	if ir, err = indexer.NewIndexer("/tmp/indexer_test", true); err != nil {
		return
	}

	//insert documents
	for _, docProt := range docProts {
		if err = ir.CreateIndex(docProt); err != nil {
			return
		}
		for i := 0; i < numDocs; i++ {
			docProt.DocID = uint64(i)
			for j := 0; j < len(docProt.UintProps); j++ {
				docProt.UintProps[j].Val = uint64(i * (j + 1))
			}
			for j := 0; j < len(docProt.StrProps); j++ {
				docProt.StrProps[j].Val = fmt.Sprintf("%03d%03d ", i, j) + "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
			}
			if err = ir.Insert(docProt); err != nil {
				return
			}
		}
	}
	return
}

func main() {
	flag.Parse()
	N := 1000000
	S := 1000

	if "" != *pprof {
		log.Printf("bootstrap: start pprof at: %s", *pprof)
		go func() {
			log.Fatalf("bootstrap: start pprof failed, errors:\n%+v",
				http.ListenAndServe(*pprof, nil))
		}()
	}

	var ir *indexer.Indexer
	var err error

	// record time
	t0 := time.Now()

	if ir, err = prepareIndexer(N, []*cql.DocumentWithIdx{newDocProt1()}); err != nil {
		log.Fatalf("%+v", err)
	}

	// record time, and calculate performance
	t1 := time.Now()
	log.Printf("duration %v", t1.Sub(t0))
	log.Printf("insertion speed %f docs/s", float64(N)/t1.Sub(t0).Seconds())

	for i := 0; i < S; i++ {
		if err = ir.Sync(); err != nil {
			log.Fatalf("%+v", err)
		}
	}

	// record time, and calculate performance
	t2 := time.Now()
	log.Printf("duration %v", t2.Sub(t1))
	log.Printf("sync speed %f syncs/s", float64(S)/t2.Sub(t1).Seconds())
}
