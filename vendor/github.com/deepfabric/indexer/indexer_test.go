package indexer

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepfabric/indexer/cql"
)

const (
	InitialNumDocs int = 100000 //insert some documents before benchmark
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

func newDocProt2() *cql.DocumentWithIdx {
	return &cql.DocumentWithIdx{
		Document: cql.Document{
			DocID: 0,
			UintProps: []cql.UintProp{
				cql.UintProp{
					Name:   "ip",
					ValLen: 4,
					Val:    0,
				},
				cql.UintProp{
					Name:   "created",
					ValLen: 8,
					Val:    0,
				},
				cql.UintProp{
					Name:   "updated",
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
		Index: "addrs",
	}
}

//TESTCASE: normal operation sequence: new_empty, create, insert, close, new_existing, query, del, destroy
func TestIndexerNormal(t *testing.T) {
	var err error
	var docProt *cql.DocumentWithIdx
	var ir, ir2 *Indexer
	var found bool

	//create empty indexer
	if ir, err = NewIndexer("/tmp/indexer_test", true); err != nil {
		t.Fatalf("%+v", err)
	}

	//create index 1
	docProt = newDocProt1()
	if err = ir.CreateIndex(docProt); err != nil {
		return
	}

	//create index 2
	docProt = newDocProt2()
	if err = ir.CreateIndex(docProt); err != nil {
		return
	}

	//insert documents
	for i := 0; i < InitialNumDocs; i++ {
		doc := newDocProt1()
		doc.DocID = uint64(i)
		for j := 0; j < len(doc.UintProps); j++ {
			doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		if err = ir.Insert(doc); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	//close indexer
	if err = ir.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	//create indexer with existing data
	if ir2, err = NewIndexer("/tmp/indexer_test", false); err != nil {
		t.Fatalf("%+v", err)
	}

	//query
	var qr *QueryResult
	low := 30
	high := 600
	cs := &cql.CqlSelect{
		Index: "orders",
		UintPreds: map[string]cql.UintPred{
			"price": cql.UintPred{
				Name: "price",
				Low:  uint64(low),
				High: uint64(high),
			},
		},
	}
	if qr, err = ir2.Select(cs); err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println(qr.Bm.Bits())

	//delete documents
	for i := 0; i < InitialNumDocs; i++ {
		doc := newDocProt1()
		doc.DocID = uint64(i)
		for j := 0; j < len(doc.UintProps); j++ {
			doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		if found, err = ir2.Del(doc.Index, doc.DocID); err != nil {
			t.Fatalf("%+v", err)
		} else if !found {
			t.Fatalf("document %v not found", doc)
		}
	}

	//destroy index 1
	if err = ir2.DestroyIndex("orders"); err != nil {
		t.Fatalf("%+v", err)
	}

	//destroy index 2
	if err = ir2.DestroyIndex("addrs"); err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestIndexerOpenClose(t *testing.T) {
	var err error
	var ir *Indexer

	//create indexer
	if ir, err = NewIndexer("/tmp/indexer_test", true); err != nil {
		t.Fatalf("%+v", err)
	}

	//create index
	docProt := newDocProt1()
	if err = ir.CreateIndex(docProt); err != nil {
		return
	}

	//close indexer
	if err = ir.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	//open indexer
	if err = ir.Open(); err != nil {
		t.Fatalf("%+v", err)
	}

	//close indexer
	if err = ir.Close(); err != nil {
		t.Fatalf("%+v", err)
	}
}

func prepareIndexer(numDocs int, docProts []*cql.DocumentWithIdx) (ir *Indexer, err error) {
	//create indexer
	if ir, err = NewIndexer("/tmp/indexer_test", true); err != nil {
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

func TestIndexerParallel(t *testing.T) {
	var err error
	var ir *Indexer
	initialNumDocs := 10000
	parallelism := 5
	duration := 120 * time.Second
	durationIns := duration + 3*time.Second //insertion runs longger than deletion to ensure deletion succeed

	//https://golang.org/pkg/testing/#hdr-Subtests_and_Sub_benchmarks
	//"The Run methods of T and B allow defining subtests and sub-benchmarks, ...provides a way to share common setup and tear-down code"
	//"A subbenchmark is like any other benchmark. A benchmark that calls Run at least once will not be measured itself and will be called once with N=1."
	//prepareIndexer is expensive setup, so it's better to share among sub-benchmarks.
	ir, err = prepareIndexer(initialNumDocs, []*cql.DocumentWithIdx{newDocProt1(), newDocProt2()})
	if err != nil {
		t.Fatalf("%+v", err)
	}

	end := time.Now().Add(duration)
	endIns := end.Add(3 * time.Second)
	seqIns := []uint64{uint64(initialNumDocs - 1), uint64(initialNumDocs - 1)}
	seqDel := []uint64{0, 0}
	var errIns, errDel, errSel error
	//Concurrently insert to index 1 and 2
	for i := 0; i < parallelism; i++ {
		go func() {
			var err2 error
			docs := []*cql.DocumentWithIdx{newDocProt1(), newDocProt2()}
			for {
				if now := time.Now(); now.After(endIns) {
					return
				}
				for d, doc := range docs {
					seq := atomic.AddUint64(&seqIns[d], 1)
					doc.DocID = seq
					for j := 0; j < len(doc.UintProps); j++ {
						doc.UintProps[j].Val = doc.DocID * uint64(j+1)
					}
					for j := 0; j < len(doc.StrProps); j++ {
						doc.StrProps[j].Val = "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
					}
					if err2 = ir.Insert(doc); err2 != nil {
						errIns = err2
					}
				}
			}
		}()
	}
	//Concurrently delete to index 1 and 2
	for i := 0; i < parallelism; i++ {
		go func() {
			var err2 error
			docs := []*cql.DocumentWithIdx{newDocProt1(), newDocProt2()}
			for {
				if now := time.Now(); now.After(end) {
					return
				}
				for d, doc := range docs {
					seq := atomic.AddUint64(&seqDel[d], 1)
					doc.DocID = seq
					for j := 0; j < len(doc.UintProps); j++ {
						doc.UintProps[j].Val = doc.DocID * uint64(j+1)
					}
					for j := 0; j < len(doc.StrProps); j++ {
						doc.StrProps[j].Val = "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
					}
					if _, err2 = ir.Del(doc.Index, doc.DocID); err2 != nil {
						//deletion could be scheduled more often than insertion.
						errDel = err2
					}
				}
			}
		}()
	}
	//Concurrently query to index 1 and 2
	for i := 0; i < parallelism; i++ {
		go func() {
			var err2 error
			low := 30
			high := 600
			queries := []*cql.CqlSelect{
				&cql.CqlSelect{
					Index: "orders",
					UintPreds: map[string]cql.UintPred{
						"price": cql.UintPred{
							Name: "price",
							Low:  uint64(low),
							High: uint64(high),
						},
					},
					StrPreds: map[string]cql.StrPred{
						"note": cql.StrPred{
							Name:     "note",
							ContWord: "017001",
						},
					},
				},
				&cql.CqlSelect{
					Index: "addrs",
					StrPreds: map[string]cql.StrPred{
						"description": cql.StrPred{
							Name:     "description",
							ContWord: "017000",
						},
					},
				},
			}
			for {
				if now := time.Now(); now.After(end) {
					return
				}
				for _, query := range queries {
					if _, err2 = ir.Select(query); err2 != nil {
						errSel = err2
					}
				}
			}
		}()
	}
	//wait goroutines to quit
	time.Sleep(durationIns)
	fmt.Printf("errIns %v, errDel %v, errSel %v\n", errIns, errDel, errSel)
	return
}

func BenchmarkIndexer(b *testing.B) {
	var err error
	var ir *Indexer

	//https://golang.org/pkg/testing/#hdr-Subtests_and_Sub_benchmarks
	//"The Run methods of T and B allow defining subtests and sub-benchmarks, ...provides a way to share common setup and tear-down code"
	//"A subbenchmark is like any other benchmark. A benchmark that calls Run at least once will not be measured itself and will be called once with N=1."
	//prepareIndexer is expensive setup, so it's better to share among sub-benchmarks.
	ir, err = prepareIndexer(InitialNumDocs, []*cql.DocumentWithIdx{newDocProt1(), newDocProt2()})
	if err != nil {
		b.Fatalf("%+v", err)
	}

	b.Run("Insert", func(b *testing.B) {
		//insert documents
		doc := newDocProt1()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			doc.DocID = uint64(InitialNumDocs + i)
			for j := 0; j < len(doc.UintProps); j++ {
				doc.UintProps[j].Val = doc.DocID * uint64(j+1)
			}
			for j := 0; j < len(doc.StrProps); j++ {
				doc.StrProps[j].Val = "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
			}
			if err = ir.Insert(doc); err != nil {
				//document could already be there since the benchmark function is called "at least once".
				//b.Fatalf("%+v", err)
			}
		}
	})

	b.Run("Query", func(b *testing.B) {
		//query documents
		var qr *QueryResult
		low := 30
		high := 600
		cs := &cql.CqlSelect{
			Index: "orders",
			UintPreds: map[string]cql.UintPred{
				"price": cql.UintPred{
					Name: "price",
					Low:  uint64(low),
					High: uint64(high),
				},
			},
			StrPreds: map[string]cql.StrPred{
				"note": cql.StrPred{
					Name:     "note",
					ContWord: "017001",
				},
			},
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if qr, err = ir.Select(cs); err != nil {
				b.Fatalf("%+v", err)
			}
			if qr.Bm.Count() == 0 {
				b.Fatalf("rb.Count() is zero")
			}
		}
	})
	return
}
