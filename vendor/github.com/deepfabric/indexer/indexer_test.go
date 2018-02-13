package indexer

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/deepfabric/indexer/cql"
)

func newDocProt1() *cql.DocumentWithIdx {
	return &cql.DocumentWithIdx{
		Doc: cql.Document{
			DocID: 0,
			UintProps: []*cql.UintProp{
				&cql.UintProp{
					Name:   "object",
					ValLen: 8,
					Val:    0,
				},
				&cql.UintProp{
					Name:   "price",
					ValLen: 4,
					Val:    0,
				},
				&cql.UintProp{
					Name:   "number",
					ValLen: 4,
					Val:    0,
				},
				&cql.UintProp{
					Name:   "date",
					ValLen: 8,
					Val:    0,
				},
			},
			StrProps: []*cql.StrProp{
				&cql.StrProp{
					Name: "description",
					Val:  "",
				},
				&cql.StrProp{
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
		Doc: cql.Document{
			DocID: 0,
			UintProps: []*cql.UintProp{
				&cql.UintProp{
					Name:   "ip",
					ValLen: 4,
					Val:    0,
				},
				&cql.UintProp{
					Name:   "created",
					ValLen: 8,
					Val:    0,
				},
				&cql.UintProp{
					Name:   "updated",
					ValLen: 8,
					Val:    0,
				},
			},
			StrProps: []*cql.StrProp{
				&cql.StrProp{
					Name: "description",
					Val:  "",
				},
				&cql.StrProp{
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
	initialNumDocs := 137

	//create empty indexer
	ir, err = NewIndexer("/tmp/indexer_test", true, false)
	require.NoError(t, err)

	//create index 1
	docProt = newDocProt1()
	err = ir.CreateIndex(docProt)
	require.NoError(t, err)

	//create index 1 again. shall be ok.
	docProt = newDocProt1()
	err = ir.CreateIndex(docProt)
	require.NoError(t, err)

	//create index 2
	docProt = newDocProt2()
	err = ir.CreateIndex(docProt)
	require.NoError(t, err)

	//insert documents
	for i := 0; i < initialNumDocs; i++ {
		doc := newDocProt1()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		err = ir.Insert(doc)
		require.NoError(t, err)
	}

	//close indexer
	err = ir.Close()
	require.NoError(t, err)

	//create indexer with existing data
	ir2, err = NewIndexer("/tmp/indexer_test", false, false)
	require.NoError(t, err)

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
	qr, err = ir2.Select(cs)
	require.NoError(t, err)
	fmt.Println(qr.Bm.Bits())

	//delete documents
	for i := 0; i < initialNumDocs; i++ {
		doc := newDocProt1()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		found, err = ir2.Del(doc.Index, doc.Doc.DocID)
		require.NoError(t, err)
		require.Equal(t, true, found)
	}

	//destroy index 1
	err = ir2.DestroyIndex("orders")
	require.NoError(t, err)

	//destroy index 2
	err = ir2.DestroyIndex("addrs")
	require.NoError(t, err)
}

//TESTCASE: corner case
func TestIndexerCorner(t *testing.T) {
	var err error
	var docProt1, docProt2 *cql.DocumentWithIdx
	var ir *Indexer
	initialNumDocs := 137

	//create empty indexer
	ir, err = NewIndexer("/tmp/indexer_test", true, false)
	require.NoError(t, err)

	//create index 1
	docProt1 = newDocProt1()
	err = ir.CreateIndex(docProt1)
	require.NoError(t, err)

	//insert docProt2 shall fail
	docProt2 = newDocProt2()
	err = ir.Insert(docProt2)
	require.Equal(t, errors.Cause(err), ErrIdxNotExist)

	//create index 1 again with different schema. shall be ok.
	docProt2.Index = docProt1.Index
	err = ir.CreateIndex(docProt2)
	require.NoError(t, err)

	//insert documents
	for i := 0; i < initialNumDocs; i++ {
		doc := newDocProt2()
		doc.Index = docProt1.Index
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		err = ir.Insert(doc)
		require.NoError(t, err)
	}

	//reinsert a document shall fail
	doc := newDocProt2()
	doc.Index = docProt1.Index
	err = ir.Insert(doc)
	require.Equal(t, errors.Cause(err), ErrDocExist)
}

//TESTCASE: normal operation sequence with wal: new_empty, create, insert, close, new_existing, query, del, destroy
func TestIndexerWal(t *testing.T) {
	var err error
	var docProt *cql.DocumentWithIdx
	var ir, ir2 *Indexer
	var found bool
	initialNumDocs := 137

	//create empty indexer
	ir, err = NewIndexer("/tmp/indexer_test", true, true)
	require.NoError(t, err)

	//create index 1
	docProt = newDocProt1()
	err = ir.CreateIndex(docProt)
	require.NoError(t, err)

	//insert documents
	for i := 0; i < initialNumDocs; i++ {
		doc := newDocProt1()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		err = ir.Insert(doc)
		require.NoError(t, err)
	}

	//close indexer
	err = ir.Close()
	require.NoError(t, err)

	//create indexer with existing data
	ir2, err = NewIndexer("/tmp/indexer_test", false, true)
	require.NoError(t, err)

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
	qr, err = ir2.Select(cs)
	require.NoError(t, err)
	fmt.Println(qr.Bm.Bits())
	require.NotEqual(t, 0, qr.Bm.Count())

	//delete documents
	for i := 0; i < initialNumDocs; i++ {
		doc := newDocProt1()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		found, err = ir2.Del(doc.Index, doc.Doc.DocID)
		require.NoError(t, err)
		require.Equal(t, true, found)
	}

	//destroy index 1
	err = ir2.DestroyIndex("orders")
	require.NoError(t, err)

	//close ir2
	err = ir2.Close()
	require.NoError(t, err)

	err = os.RemoveAll("/tmp/indexer_test")
	require.NoError(t, err)

	//create indexer with existing empty data
	_, err = NewIndexer("/tmp/indexer_test", false, true)
	require.NoError(t, err)
}

func TestIndexerSnapEmpty(t *testing.T) {
	var err error
	var ir, ir2 *Indexer
	var numList []uint64
	mainDir1 := "/tmp/indexer_test"
	mainDir2 := "/tmp/indexer_test2"
	snapDir := "/tmp/indexer_test_snap"

	err = os.RemoveAll(mainDir1)
	require.NoError(t, err)
	err = os.RemoveAll(mainDir2)
	require.NoError(t, err)
	err = os.RemoveAll(snapDir)
	require.NoError(t, err)

	//create empty indexer
	ir, err = NewIndexer(mainDir1, false, false)
	require.NoError(t, err)

	err = ir.ApplySnapshot(snapDir)
	require.NoError(t, err)
	fmt.Printf("ir.GetDocProts(): %v\n", ir.GetDocProts())

	numList, err = ir.CreateSnapshot(snapDir)
	require.NoError(t, err)
	require.Equal(t, []uint64{}, numList)

	//create indexer with existing data
	ir2, err = NewIndexer(mainDir2, false, false)
	require.NoError(t, err)
	err = ir2.ApplySnapshot(snapDir)
	require.NoError(t, err)

	err = os.RemoveAll(mainDir1)
	require.NoError(t, err)
	err = os.RemoveAll(mainDir2)
	require.NoError(t, err)

	//create empty indexer
	ir, err = NewIndexer(mainDir1, false, true)
	require.NoError(t, err)

	numList, err = ir.CreateSnapshot(snapDir)
	require.NoError(t, err)
	require.Equal(t, []uint64{}, numList)

	//create indexer with existing data
	ir2, err = NewIndexer(mainDir2, false, true)
	require.NoError(t, err)
	err = ir2.ApplySnapshot(snapDir)
	require.NoError(t, err)
}

func TestIndexerSnap(t *testing.T) {
	var err error
	var docProt *cql.DocumentWithIdx
	var ir, ir2 *Indexer
	var numList, numList2 []uint64
	initialNumDocs := 137
	mainDir1 := "/tmp/indexer_test"
	mainDir2 := "/tmp/indexer_test2"
	snapDir := "/tmp/indexer_test_snap"

	err = os.RemoveAll(mainDir1)
	require.NoError(t, err)
	err = os.RemoveAll(mainDir2)
	require.NoError(t, err)

	//create empty indexer
	ir, err = NewIndexer(mainDir1, false, false)
	require.NoError(t, err)

	//create index 1
	docProt = newDocProt1()
	err = ir.CreateIndex(docProt)
	require.NoError(t, err)

	//insert documents
	for i := 0; i < initialNumDocs; i++ {
		doc := newDocProt1()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		err = ir.Insert(doc)
		require.NoError(t, err)
	}

	numList, err = ir.CreateSnapshot(snapDir)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, numList)

	numList2 = ir.GetDocIDFragList()
	require.Equal(t, numList, numList2)

	//create index 2
	docProt = newDocProt2()
	err = ir.CreateIndex(docProt)
	require.NoError(t, err)

	doc := newDocProt2()
	doc.Doc.DocID = pilosa.SliceWidth
	err = ir.Insert(doc)
	require.NoError(t, err)

	numList, err = ir.CreateSnapshot(snapDir)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, numList)

	numList2 = ir.GetDocIDFragList()
	require.Equal(t, numList, numList2)

	//create indexer with existing data
	ir2, err = NewIndexer(mainDir2, false, false)
	require.NoError(t, err)
	err = ir2.ApplySnapshot(snapDir)
	require.NoError(t, err)
	fmt.Printf("ir2.GetDocProts(): %v\n", ir2.GetDocProts())

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
	qr, err = ir2.Select(cs)
	require.NoError(t, err)
	fmt.Println(qr.Bm.Bits())
	require.NotEqual(t, 0, qr.Bm.Count())
}

func TestIndexerOpenClose(t *testing.T) {
	var err error
	var ir *Indexer

	//create indexer
	ir, err = NewIndexer("/tmp/indexer_test", true, false)
	require.NoError(t, err)

	//create index
	docProt := newDocProt1()
	err = ir.CreateIndex(docProt)
	require.NoError(t, err)

	//close indexer
	err = ir.Close()
	require.NoError(t, err)

	//open indexer
	err = ir.Open()
	require.NoError(t, err)

	//close indexer
	err = ir.Close()
	require.NoError(t, err)
}

func prepareIndexer(numDocs int, docProts []*cql.DocumentWithIdx) (ir *Indexer, err error) {
	//create indexer
	if ir, err = NewIndexer("/tmp/indexer_test", true, false); err != nil {
		return
	}

	//insert documents
	for _, docProt := range docProts {
		if err = ir.CreateIndex(docProt); err != nil {
			return
		}
		for i := 0; i < numDocs; i++ {
			docProt.Doc.DocID = uint64(i)
			for j := 0; j < len(docProt.Doc.UintProps); j++ {
				docProt.Doc.UintProps[j].Val = uint64(i * (j + 1))
			}
			for j := 0; j < len(docProt.Doc.StrProps); j++ {
				docProt.Doc.StrProps[j].Val = fmt.Sprintf("%03d%03d ", i, j) + "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
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
	require.NoError(t, err)

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
					doc.Doc.DocID = seq
					for j := 0; j < len(doc.Doc.UintProps); j++ {
						doc.Doc.UintProps[j].Val = doc.Doc.DocID * uint64(j+1)
					}
					for j := 0; j < len(doc.Doc.StrProps); j++ {
						doc.Doc.StrProps[j].Val = "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
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
					doc.Doc.DocID = seq
					for j := 0; j < len(doc.Doc.UintProps); j++ {
						doc.Doc.UintProps[j].Val = doc.Doc.DocID * uint64(j+1)
					}
					for j := 0; j < len(doc.Doc.StrProps); j++ {
						doc.Doc.StrProps[j].Val = "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
					}
					if _, err2 = ir.Del(doc.Index, doc.Doc.DocID); err2 != nil {
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
	initialNumDocs := 10000

	//https://golang.org/pkg/testing/#hdr-Subtests_and_Sub_benchmarks
	//"The Run methods of T and B allow defining subtests and sub-benchmarks, ...provides a way to share common setup and tear-down code"
	//"A subbenchmark is like any other benchmark. A benchmark that calls Run at least once will not be measured itself and will be called once with N=1."
	//prepareIndexer is expensive setup, so it's better to share among sub-benchmarks.
	ir, err = prepareIndexer(initialNumDocs, []*cql.DocumentWithIdx{newDocProt1(), newDocProt2()})
	require.NoError(b, err)

	b.Run("Insert", func(b *testing.B) {
		//insert documents
		doc := newDocProt1()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			doc.Doc.DocID = uint64(initialNumDocs + i)
			for j := 0; j < len(doc.Doc.UintProps); j++ {
				doc.Doc.UintProps[j].Val = doc.Doc.DocID * uint64(j+1)
			}
			for j := 0; j < len(doc.Doc.StrProps); j++ {
				doc.Doc.StrProps[j].Val = "Go's standard library does not have a function solely intended to check if a file exists or not (like Python's os.path.exists). What is the idiomatic way to do it?"
			}
			err = ir.Insert(doc)
			//document could already be there since the benchmark function is called "at least once".
			//require.NoError(b, err)
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
			qr, err = ir.Select(cs)
			require.NoError(b, err)
			require.NotEqual(b, 0, qr.Bm.Count())
		}
	})
	return
}
