package indexer

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/skshmgpt/srch/internal"
	"github.com/skshmgpt/srch/internal/storage"
)

// read from a pool of processed Docs
// build posting
// using the storeindexpart api, writes index for a subset of Docs
// store the meta of the
//

func worker(batch Batch, id int, retryChan chan Batch) *internal.IdxSegment {
	log.Printf("worker %d processing batch %d\n", id, batch.ID)
	start := time.Now()
	idx := internal.Index{
		InvIdx:   make(map[string]*internal.PostingList),
		Df:       make(map[string]uint32),
		DocCount: uint64(len(batch.Docs)),
		DocLen:   make(map[uint32]uint32),
	}

	termMap := make(map[string]uint32, 50000)
	terms := make([]string, 0, 50000)
	postings := make([]internal.PostingList, 0, 50000)
	df := make([]uint32, 0, 50000)

	for _, p := range batch.Docs {
		for _, tf := range p.Terms {
			termId, ok := termMap[tf.Term]
			if !ok {
				termId = uint32(len(terms))
				termMap[tf.Term] = termId
				terms = append(terms, tf.Term)
				postings = append(postings, internal.PostingList{
					DocIDS:     make([]uint32, 0, 16),
					FreqsTitle: make([]uint32, 0, 16),
					FreqsBody:  make([]uint32, 0, 16),
				})
				df = append(df, 0)
			}
			pl := &postings[termId]
			pl.DocIDS = append(pl.DocIDS, p.ID)
			pl.FreqsTitle = append(pl.FreqsTitle, tf.FreqTitle)
			pl.FreqsBody = append(pl.FreqsBody, tf.FreqBody)
			df[termId]++
		}
		docLen := p.TitleLen + p.BodyLen
		idx.DocLen[p.ID] = docLen
		idx.TotalDoclen += uint64(docLen)
		idx.TotalTitleLen += uint64(p.TitleLen)
		idx.TotalBodyLen += uint64(p.BodyLen)
	}

	for termId, term := range terms {
		idx.InvIdx[term] = &postings[termId]
		idx.Df[term] = df[termId]
	}
	clear(termMap)
	clear(terms)
	clear(df)

	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	// in indexer/indexer.go worker(), wrap the write call:
	buildTime := time.Since(start)
	t0 := time.Now()
	meta, err := storage.WriteIdxPart(&idx, batch.ID, dir)
	log.Printf("batch %d: build=%v write=%v terms=%d pages=%d", batch.ID, buildTime, time.Since(t0), len(idx.Df), len(batch.Docs))

	if err != nil {
		if batch.Retries >= 2 {
			// skip batch
			log.Printf("Error processing batch, max retries reached, skipping : worker %d, err : %v", id, err)
		} else {
			batch.Retries++
			retryChan <- batch
			log.Printf("Error processing batch : worker %d, err : %v", id, err)
		}
	}

	return meta
}

type Batch struct {
	ID      int
	Docs    []internal.ProcessedDoc
	Retries int
}

func Index(batchSize int, workers int, procPgPool <-chan internal.ProcessedDoc) {

	var wg sync.WaitGroup
	var b Batch
	batchId := 1
	b.Docs = make([]internal.ProcessedDoc, 0, batchSize)
	batchChan := make(chan Batch, workers*2)
	retryChan := make(chan Batch, workers*2)

	go func() {
		for b := range retryChan {
			batchChan <- b
		}
	}()

	for i := range workers {
		wg.Go(func() {
			for batch := range batchChan {
				worker(batch, i, retryChan)
			}
		})
	}

	for p := range procPgPool {
		b.Docs = append(b.Docs, p)

		if len(b.Docs) == batchSize {
			batchChan <- Batch{
				ID:      batchId,
				Docs:    append([]internal.ProcessedDoc(nil), b.Docs...),
				Retries: b.Retries,
			}
			batchId++
			b.Docs = make([]internal.ProcessedDoc, 0, batchSize)
		}
	}

	if len(b.Docs) > 0 {
		batchChan <- Batch{
			ID:      batchId,
			Docs:    append([]internal.ProcessedDoc(nil), b.Docs...),
			Retries: b.Retries,
		}
	}

	close(batchChan)
	clear(b.Docs)
	wg.Wait()
}
