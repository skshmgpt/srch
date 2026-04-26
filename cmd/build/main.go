package main

import (
	"log"
	"sync"
	"time"

	"github.com/skshmgpt/srch/internal"
	"github.com/skshmgpt/srch/internal/indexer"
	"github.com/skshmgpt/srch/internal/tokenizer"
)

func main() {

	proc_pgPool := make(chan internal.ProcessedDoc, 5000)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tokenizer.Tokenize(4, "enwiki-part.bz2", proc_pgPool)
	}()

	idxStart := time.Now()
	go func() {
		defer wg.Done()
		indexer.Index(30000, 1, proc_pgPool)
		took := time.Since(idxStart)
		log.Printf("indexer took %.2fs", took.Seconds())
	}()

	wg.Wait()

}
