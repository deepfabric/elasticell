package indexer

import (
	"fmt"
	"testing"

	datastructures "github.com/deepfabric/go-datastructures"
	"github.com/deepfabric/indexer/cql"
	"github.com/juju/testing/checkers"
)

const (
	NumDocs = 10000
)

func newDocProt() *cql.DocumentWithIdx {
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
					Name:    "priceF64",
					IsFloat: true,
					ValLen:  8,
					Val:     0,
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

//TESTCASE: normal operation sequence: create, insert, del, destroy
func TestIndexNormal(t *testing.T) {
	var err error
	var ind *Index
	var isEqual bool
	var found bool
	var bits map[uint64][]uint64

	docProt := newDocProt()
	if ind, err = NewIndex(docProt, "/tmp/index_test"); err != nil {
		t.Fatalf("incorrect result of NewIndex, %+v", err)
	}
	if isEqual, err = checkers.DeepEqual(ind.DocProt, docProt); !isEqual {
		t.Fatalf("incorrect result of NewIndex, %+v", err)
	}

	for i := 0; i < NumDocs; i++ {
		doc := newDocProt()
		doc.DocID = uint64(i)
		for j := 0; j < len(doc.UintProps); j++ {
			val := uint64(i * (j + 1))
			if val, err = cql.ParseUintProp(doc.UintProps[j], fmt.Sprintf("%v", val)); err != nil {
				t.Fatalf("%+v", err)
			}
			doc.UintProps[j].Val = val
		}
		for j := 0; j < len(doc.StrProps); j++ {
			doc.StrProps[j].Val = fmt.Sprintf("%03d%03d and some random text", i, j)
		}
		if err = ind.Insert(doc); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	// query numerical(integer) range
	var qr *QueryResult
	var items []datastructures.Comparable
	low := uint64(30)
	high := uint64(600)
	cs := &cql.CqlSelect{
		Index: docProt.Index,
		UintPreds: map[string]cql.UintPred{
			"price": cql.UintPred{
				Name: "price",
				Low:  low,
				High: high,
			},
		},
	}
	if qr, err = ind.Select(cs); err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Printf("query result: %v\n", qr.Bm.Bits())
	// low <= 2*i <= high, (low+1)/2 <= i <= high/2
	want := int(high/2 - (low+1)/2 + 1)
	if qr.Bm.Count() != uint64(want) {
		t.Fatalf("incorrect number of matches, have %d, want %d", qr.Bm.Count(), want)
	}

	// query numerical range + order by + text
	cs.OrderBy = "price"
	cs.Limit = 20
	if qr, err = ind.Select(cs); err != nil {
		t.Fatalf("%+v", err)
	}
	items = qr.Oa.Finalize()
	fmt.Printf("query result: %v\n", items)
	want = cs.Limit
	if len(items) != want {
		t.Fatalf("incorrect number of matches, have %d, want %d", len(items), want)
	}

	// dump bits
	for name, frame := range ind.txtFrames {
		var termID uint64
		if termID, found = frame.td.GetTermID("017001"); !found {
			continue
		}
		if bits, err = frame.Bits(); err != nil {
			t.Fatalf("%+v", err)
		}
		//fmt.Printf("frmae %v bits: %v\n", name, bits)
		fmt.Printf("frame %v bits[%v]: %v\n", name, termID, bits[termID])
	}

	// query numerical range + text
	cs.StrPreds = map[string]cql.StrPred{
		"note": cql.StrPred{
			Name:     "note",
			ContWord: "017001",
			//			ContWord: "random",
		},
	}
	cs.Limit = 20
	if qr, err = ind.Select(cs); err != nil {
		t.Fatalf("%+v", err)
	}
	items = qr.Oa.Finalize()
	fmt.Printf("query result: %v\n", items)
	want = 1
	if len(items) != want {
		t.Fatalf("incorrect number of matches, have %d, want %d", len(items), want)
	}

	// query numerical(float) range
	valSs := []string{"30", "600"}
	vals := make([]uint64, len(valSs))
	for i, valS := range valSs {
		var val uint64
		if val, err = cql.Float64ToSortableUint64(valS); err != nil {
			t.Fatalf("%+v", err)
		}
		vals[i] = val
		fmt.Printf("FLOAT64 %v\t%v\n", valS, val)
	}
	low, high = vals[0], vals[1]
	cs = &cql.CqlSelect{
		Index: docProt.Index,
		UintPreds: map[string]cql.UintPred{
			"priceF64": cql.UintPred{
				Name: "priceF64",
				Low:  low,
				High: high,
			},
		},
	}
	if qr, err = ind.Select(cs); err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Printf("query result: %v\n", qr.Bm.Bits())
	// low <= 3*i <= high, (low+2)/3 <= i <= high/3
	want = int(600/3 - (30+2)/3 + 1)
	if qr.Bm.Count() != uint64(want) {
		t.Fatalf("incorrect number of matches, have %d, want %d", qr.Bm.Count(), want)
	}

	//delete docs
	for i := 0; i < NumDocs; i++ {
		doc := newDocProt()
		doc.DocID = uint64(i)
		for j := 0; j < len(doc.UintProps); j++ {
			doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		if found, err = ind.Del(doc.DocID); err != nil {
			t.Fatalf("%+v", err)
		} else if !found {
			t.Fatalf("document %v not found", doc)
		}
	}
}

func TestIndexOpenClose(t *testing.T) {
	var err error
	var ind, ind2 *Index
	var isEqual bool

	//create index
	docProt := newDocProt()
	ind, err = NewIndex(docProt, "/tmp/index_test")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	//insert documents
	for i := 0; i < NumDocs; i++ {
		doc := newDocProt()
		doc.DocID = uint64(i)
		for j := 0; j < len(doc.UintProps); j++ {
			doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		if err = ind.Insert(doc); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	//close index
	if err = ind.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	//open index
	if err = ind.Open(); err != nil {
		t.Fatalf("%+v", err)
	}

	//close index
	if err = ind.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	//open index with another Index object. This occurs when program restart.
	ind2, err = NewIndexExt("/tmp/index_test", "orders")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	//verify DocProt keeps unchanged
	if isEqual, err = checkers.DeepEqual(ind2.DocProt, ind.DocProt); !isEqual {
		fmt.Printf("have %v\n", ind2.DocProt)
		fmt.Printf("want %v\n", ind.DocProt)
		t.Fatalf("index DocProt %+v", err)
	}

	//close index
	if err = ind2.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	//destroy index
	if err = ind.Destroy(); err != nil {
		t.Fatalf("%+v", err)
	}
}
