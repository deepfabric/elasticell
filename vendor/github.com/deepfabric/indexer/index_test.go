package indexer

import (
	"fmt"
	"testing"

	datastructures "github.com/deepfabric/go-datastructures"
	"github.com/deepfabric/indexer/cql"
	"github.com/stretchr/testify/require"
)

const (
	NumDocs = 10000
)

func newDocProt() *cql.DocumentWithIdx {
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
					Name:    "priceF64",
					IsFloat: true,
					ValLen:  8,
					Val:     0,
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

//TESTCASE: normal operation sequence: create, insert, del, destroy
func TestIndexNormal(t *testing.T) {
	var err error
	var ind *Index
	var found bool
	var bits map[uint64][]uint64

	docProt := newDocProt()
	ind, err = NewIndex(docProt, "/tmp/index_test")
	require.NoError(t, err)
	require.Equal(t, docProt, ind.DocProt)
	for i := 0; i < NumDocs; i++ {
		doc := newDocProt()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			val := uint64(i * (j + 1))
			val, err = cql.ParseUintProp(doc.Doc.UintProps[j], fmt.Sprintf("%v", val))
			require.NoError(t, err)
			doc.Doc.UintProps[j].Val = val
		}
		for j := 0; j < len(doc.Doc.StrProps); j++ {
			doc.Doc.StrProps[j].Val = fmt.Sprintf("%03d%03d and some random text", i, j)
		}
		err = ind.Insert(doc)
		require.NoError(t, err)
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
	qr, err = ind.Select(cs)
	require.NoError(t, err)
	fmt.Printf("query result: %v\n", qr.Bm.Bits())
	// low <= 2*i <= high, (low+1)/2 <= i <= high/2
	want := uint64(high/2 - (low+1)/2 + 1)
	require.Equalf(t, want, qr.Bm.Count(), "incorrect number of matches")

	// query numerical range + order by + text
	cs.OrderBy = "price"
	cs.Limit = 20
	qr, err = ind.Select(cs)
	require.NoError(t, err)

	items = qr.Oa.Finalize()
	fmt.Printf("query result: %v\n", items)
	require.Equalf(t, cs.Limit, len(items), "incorrect number of matches")

	// dump bits
	for name, frame := range ind.txtFrames {
		var termID uint64
		if termID, found = frame.td.GetTermID("017001"); !found {
			continue
		}
		bits, err = frame.Bits()
		require.NoError(t, err)
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
	qr, err = ind.Select(cs)
	require.NoError(t, err)
	items = qr.Oa.Finalize()
	fmt.Printf("query result: %v\n", items)
	require.Equalf(t, 1, len(items), "incorrect number of matches")

	// query numerical(float) range
	valSs := []string{"30", "600"}
	vals := make([]uint64, len(valSs))
	for i, valS := range valSs {
		var val uint64
		val, err = cql.Float64ToSortableUint64(valS)
		require.NoError(t, err)
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
	qr, err = ind.Select(cs)
	require.NoError(t, err)
	fmt.Printf("query result: %v\n", qr.Bm.Bits())
	// low <= 3*i <= high, (low+2)/3 <= i <= high/3
	want = uint64(600/3 - (30+2)/3 + 1)
	require.Equalf(t, want, qr.Bm.Count(), "incorrect number of matches")

	//delete docs
	for i := 0; i < NumDocs; i++ {
		doc := newDocProt()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		found, err = ind.Del(doc.Doc.DocID)
		require.NoError(t, err)
		require.Equalf(t, true, found, "document %v not found", doc)
	}
}

func TestIndexOpenClose(t *testing.T) {
	var err error
	var ind, ind2 *Index

	//create index
	docProt := newDocProt()
	ind, err = NewIndex(docProt, "/tmp/index_test")
	require.NoError(t, err)

	//insert documents
	for i := 0; i < NumDocs; i++ {
		doc := newDocProt()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		err = ind.Insert(doc)
		require.NoError(t, err)
	}

	//close index
	err = ind.Close()
	require.NoError(t, err)

	//open index
	err = ind.Open()
	require.NoError(t, err)

	//close index
	err = ind.Close()
	require.NoError(t, err)

	//open index with another Index object. This occurs when program restart.
	ind2, err = NewIndexExt("/tmp/index_test", "orders")
	require.NoError(t, err)

	//verify DocProt keeps unchanged
	require.Equal(t, ind.DocProt, ind2.DocProt)

	//close index
	err = ind2.Close()
	require.NoError(t, err)

	//destroy index
	err = ind.Destroy()
	require.NoError(t, err)
}
