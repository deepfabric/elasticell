package cql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCql(t *testing.T) {
	var res interface{}
	var err error
	tcs := []string{
		"IDX.CREATE orders SCHEMA type ENUM",
		"IDX.CREATE orders SCHEMA object UINT64 price UINT32 number UINT32 date UINT64",
		"IDX.CREATE orders SCHEMA object UINT64 price UINT32 number UINT32 date UINT64 type ENUM",
		"IDX.CREATE orders SCHEMA object UINT64 price UINT32 number UINT32 date UINT64 desc STRING",
		"IDX.CREATE orders SCHEMA object UINT64 price UINT32 number UINT32 date UINT64 type ENUM desc STRING",
		"IDX.INSERT orders 615 11 22 33 44 3 \"description\"",
		"IDX.DEL orders 615 11 22 33 44 3 \"description\"",
		"IDX.SELECT orders WHERE price>=30 price<40 date<2017 type IN [1,3] desc CONTAINS \"pen\" ORDERBY date",
		"IDX.SELECT orders WHERE price>=30 price<=40 date<2017 type IN [1,3] ORDERBY date LIMIT 30",
		"IDX.SELECT orders WHERE price>=30 price<=40 type IN [1,3]",
		"QUERY orders WHERE price>=30 price<=40 type IN [1,3]",
		"IDX.DESTROY orders",
	}
	docProts := make(map[string]*Document)
	for i, tc := range tcs {
		fmt.Println(tc)
		// Note that IDX.CREATE and IDX.DEL don't need docProts.
		res, err = ParseCql(tc, docProts)
		require.NoErrorf(t, err, "case %d", i)
		switch r := res.(type) {
		case *CqlCreate:
			fmt.Printf("Create index %v\n", r)
			docProts[r.DocumentWithIdx.Index] = &r.DocumentWithIdx.Doc
		case *CqlDestroy:
			fmt.Printf("Destroy index %s\n", r.Index)
			delete(docProts, r.Index)
		case *CqlInsert:
			fmt.Printf("Insert %v\n", r)
		case *CqlDel:
			fmt.Printf("Del %v\n", r)
		case *CqlSelect:
			fmt.Printf("Select %v\n", r)
		default:
			//There shouldn't be any parsing error for above test cases.
			t.Fatalf("case %d, res %+v\n", i, res)
		}
	}
}

func TestParseCqlSelect(t *testing.T) {
	var res interface{}
	var err error
	var c *CqlCreate
	var q *CqlSelect
	var uintPred UintPred
	var enumPred EnumPred
	var strPred StrPred
	var ok bool
	//Prepare index
	docProts := make(map[string]*Document)
	res, err = ParseCql("IDX.CREATE orders SCHEMA object UINT64 price UINT32 priceF32 FLOAT32 priceF64 FLOAT64 number UINT32 date UINT64 type ENUM desc STRING", docProts)
	require.NoError(t, err)
	c = res.(*CqlCreate)
	docProts[c.DocumentWithIdx.Index] = &c.DocumentWithIdx.Doc

	//TESTCASE: multiple UintPred of the same property into one
	res, err = ParseCql("IDX.SELECT orders WHERE price>=30 price<=40 price<35 price>20", docProts)
	require.NoError(t, err)
	q = res.(*CqlSelect)
	uintPred, ok = q.UintPreds["price"]
	require.Equalf(t, true, ok, "UintPred price is gone")
	require.Equal(t, 30, uintPred.Low)
	require.Equal(t, 34, uintPred.High)

	//TESTCASE: FLOAT32
	valSs := []string{"30", "40.3"}
	vals := make([]uint64, len(valSs))
	for i, valS := range valSs {
		var val uint64
		val, err = Float32ToSortableUint64(valS)
		require.NoError(t, err)
		vals[i] = val
		fmt.Printf("FLOAT32 %v\t%v\n", valS, val)
	}
	res, err = ParseCql("IDX.SELECT orders WHERE priceF32>=30 priceF32<=40.3", docProts)
	require.NoError(t, err)
	q = res.(*CqlSelect)
	uintPred, ok = q.UintPreds["priceF32"]
	require.Equalf(t, true, ok, "UintPred price is gone")
	require.Equal(t, vals[0], uintPred.Low)
	require.Equal(t, vals[1], uintPred.High)

	//TESTCASE: FLOAT64
	for i, valS := range valSs {
		var val uint64
		val, err = Float64ToSortableUint64(valS)
		require.NoError(t, err)
		vals[i] = val
		fmt.Printf("FLOAT64 %v\t%v\n", valS, val)
	}
	res, err = ParseCql("IDX.SELECT orders WHERE priceF64>=30 priceF64<=40.3", docProts)
	require.NoError(t, err)
	q = res.(*CqlSelect)
	uintPred, ok = q.UintPreds["priceF64"]
	require.Equalf(t, true, ok, "UintPred price is gone")
	require.Equal(t, vals[0], uintPred.Low)
	require.Equal(t, vals[1], uintPred.High)

	//TESTCASE: normal EnumPred
	res, err = ParseCql("IDX.SELECT orders WHERE type IN [1,3]", docProts)
	require.NoError(t, err)
	q = res.(*CqlSelect)
	enumPred, ok = q.EnumPreds["type"]
	require.Equalf(t, true, ok, "EnumPred type is gone")
	require.Equalf(t, []int{1, 3}, enumPred.InVals, "incorrect EnumPred type")

	//TESTCASE: invalid query due to multiple EnumPred of a property
	res, err = ParseCql("IDX.SELECT orders WHERE type IN [1,3] type IN [3,9]", docProts)
	require.Errorf(t, err, "incorrect EnumPred type %v, want error", res)

	//TESTCASE: normal StrPred
	res, err = ParseCql("IDX.SELECT orders WHERE desc CONTAINS \"pen\"", docProts)
	require.NoError(t, err)
	q = res.(*CqlSelect)
	strPred, ok = q.StrPreds["desc"]
	require.Equalf(t, true, ok, "StrPred desc is gone")
	require.Equal(t, "pen", strings.ToLower(strPred.ContWord))

	tcs := []string{
		//TESTCASE: invalid query due to multiple StrPred of a property
		"IDX.SELECT orders WHERE desc CONTAINS \"pen\" desc CONTAINS \"pencil\"",
		//TESTCASE: invalid query due to OBDERBY property doesn't occur in WHERE
		"IDX.SELECT orders WHERE price>=30 price<=40 ORDERBY date",
		//TESTCASE: invalid query due to OBDERBY property doesn't occur as a UintPred
		"IDX.SELECT orders WHERE price>=30 price<=40 type IN [1,3] ORDERBY type",
		//TESTCASE: invalid query due to mismatching property name
		"IDX.SELECT orders WHERE prices>=20.2",
	}
	for _, tc := range tcs {
		res, err = ParseCql(tc, docProts)
		require.Errorf(t, err, "have %+v, want an error", res)
	}
}
