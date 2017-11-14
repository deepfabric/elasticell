package indexer

import (
	"os"
	"testing"

	"github.com/juju/testing/checkers"
)

func TestTermDict(t *testing.T) {
	var err error
	var isEqual bool
	var td *TermDict
	var td2 *TermDict

	//TESTCASE: query and insert term to an empty dict
	if err = os.Remove("/tmp/terms"); err != nil && !os.IsNotExist(err) {
		t.Fatalf("%+v", err)
	}
	if td, err = NewTermDict("/tmp", true); err != nil {
		t.Fatalf("%+v", err)
	}
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
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if isEqual, err = checkers.DeepEqual(ids, expIds); !isEqual {
		t.Fatalf("incorrect result of (*TermDict).GetTermsID, %+v", err)
	}

	//TESTCASE: query and insert term to an existing dict
	if td2, err = NewTermDict("/tmp", false); err != nil {
		t.Fatalf("%+v", err)
	}
	terms = []string{
		"friday",
		"wikepedia",
		"thurs",
	}
	expIds = []uint64{5, 7, 4}

	ids, err = td2.CreateTermsIfNotExist(terms)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if isEqual, err = checkers.DeepEqual(ids, expIds); !isEqual {
		t.Fatalf("incorrect result of (*TermDict).GetTermsID, %+v", err)
	}
}
