package main

import (
	"log"
	"sync"

	"github.com/skshmgpt/srch/internal/indexer"
	"github.com/skshmgpt/srch/internal/tokenizer"
)

func main() {

	proc_pgPool := make(chan tokenizer.ProcessedPage, 100)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tokenizer.Tokenize("enwiki-latest-pages-articles-multistream1.xml-p1p41242.bz2", proc_pgPool)
	}()

	var index *indexer.Index

	go func() {
		defer wg.Done()
		index = indexer.BuildIndex(proc_pgPool)
	}()

	wg.Wait()
	log.Println(len(index.InvIdx["india"].DocIDS))
}
