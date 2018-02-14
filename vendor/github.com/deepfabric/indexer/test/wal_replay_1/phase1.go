package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/deepfabric/indexer"
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

func main() {
	var err error
	var docProt *cql.DocumentWithIdx
	var ir *indexer.Indexer

	//create empty indexer
	ir, err = indexer.NewIndexer("/tmp/indexer_test", true, true)
	if err != nil {
		log.Fatal(err)
	}

	//create index 1
	docProt = newDocProt1()
	err = ir.CreateIndex(docProt)
	if err != nil {
		log.Fatal(err)
	}

	//insert documents
	for i := 0; i < 137; i++ {
		doc := newDocProt1()
		doc.Doc.DocID = uint64(i)
		for j := 0; j < len(doc.Doc.UintProps); j++ {
			doc.Doc.UintProps[j].Val = uint64(i * (j + 1))
		}
		err = ir.Insert(doc)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Infoln("quit without Sync. Please verify there are some files under /tmp/indexer_test/wal.")
}
