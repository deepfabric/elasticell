package main

import (
	"github.com/deepfabric/indexer"
	"github.com/deepfabric/indexer/cql"
	log "github.com/sirupsen/logrus"
)

func main() {
	var err error
	var ir2 *indexer.Indexer

	//create indexer with existing data
	ir2, err = indexer.NewIndexer("/tmp/indexer_test", false, true)
	if err != nil {
		log.Fatal(err)
	}

	//query
	var qr *indexer.QueryResult
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
	if err != nil {
		log.Fatal(err)
	}
	log.Infoln(qr.Bm.Bits())
}
