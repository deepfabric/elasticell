package main

import (
	"fmt"
	"log"
	"os"

	"github.com/deepfabric/indexer"
)

func main() {
	var err error
	var ir *indexer.Indexer
	var sum string

	if len(os.Args) <= 1 {
		log.Fatalf("paths required\n")
	}

	for i := 1; i < len(os.Args); i++ {
		fp := os.Args[i]
		if _, err := os.Stat(fp); err != nil || os.IsNotExist(err) {
			log.Fatalf("path %s doesn't exists", fp)
		}

		if ir, err = indexer.NewIndexer(fp, false, false); err != nil {
			log.Fatalf("%+v", err)
		}

		if sum, err = ir.Summary(); err != nil {
			log.Fatalf("%+v", err)
		}

		fmt.Printf("%s\n%s\n", fp, sum)

		//close indexer
		if err = ir.Close(); err != nil {
			log.Fatalf("%+v", err)
		}
	}

}
