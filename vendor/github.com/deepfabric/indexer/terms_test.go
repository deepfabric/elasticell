package indexer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTermDict(t *testing.T) {
	var err error
	var td *TermDict
	var td2 *TermDict

	//TESTCASE: query and insert term to an empty dict
	if err = os.Remove("/tmp/terms"); err != nil && !os.IsNotExist(err) {
		t.Fatalf("%+v", err)
	}
	td, err = NewTermDict("/tmp", true)
	require.NoError(t, err)
	terms := []string{
		"sunday",
		"mon",
		"tue",
		"wen",
		"thurs",
		"friday",
		"satur",
	}
	expIds := []uint64{0, 1, 2, 3, 4, 5, 6}

	ids, err := td.CreateTermsIfNotExist(terms)
	require.NoError(t, err)
	require.Equal(t, expIds, ids)

	//TESTCASE: query and insert term to an existing dict
	td2, err = NewTermDict("/tmp", false)
	require.NoError(t, err)
	terms = []string{
		"friday",
		"wikepedia",
		"thurs",
	}
	expIds = []uint64{5, 7, 4}

	ids, err = td2.CreateTermsIfNotExist(terms)
	require.NoError(t, err)
	require.Equal(t, expIds, ids)
}
