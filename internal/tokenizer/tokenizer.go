package tokenizer

import (
	"encoding/xml"
	"io"
	"log"
	"os"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/aaaton/golem/v4"
	"github.com/aaaton/golem/v4/dicts/en"
	"github.com/d4l3k/go-pbzip2"
	"github.com/skshmgpt/srch/internal"
	"github.com/skshmgpt/srch/internal/preprocessor"
	"github.com/skshmgpt/srch/internal/storage"
)

// streams bz2 chunk from ~25gb compressed file
// decompress chunk
// parse xml -> build Doc
// hand over a Doc to a worker
// worker loops over all the text:
// 1. normalization
// 2. stop word removal
// 3. stemming
// 4. special character handling/pruning
// build a term frequency map for each document
// write the result to a global channel
//

var stopWords = map[string]struct{}{
	"a": {}, "an": {}, "the": {}, "and": {}, "or": {}, "but": {},
	"is": {}, "are": {}, "was": {}, "were": {}, "be": {}, "been": {},
	"in": {}, "on": {}, "at": {}, "to": {}, "for": {}, "of": {},
	"with": {}, "as": {}, "by": {}, "from": {}, "that": {}, "this": {},
	"it": {}, "its": {}, "he": {}, "she": {}, "they": {}, "them": {},
	"we": {}, "us": {}, "you": {}, "your": {}, "i": {}, "me": {}, "my": {},
	"do": {}, "does": {}, "did": {}, "doing": {},
	"have": {}, "has": {}, "had": {},
	"not": {}, "no": {}, "so": {}, "too": {}, "very": {},
}

func isValid(tok string) bool {
	if tok == "" || len(tok) < 2 {
		return false
	}
	if _, ok := stopWords[tok]; ok {
		return false
	}
	return true
}

// processText tokenizes text into tfm and returns the number of valid tokens
// it added (after stop-word + length filtering). The return value is the
// "field length" used by BM25 / BM25F at query time.
func processText(text string, tfm map[string]uint32, buf *[]byte, lz *golem.Lemmatizer) uint32 {
	var n uint32
	for _, r := range text {
		r = unicode.ToLower(r)
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			*buf = utf8.AppendRune(*buf, r)
		} else {
			token := string(*buf)
			if isValid(token) {
				tfm[lz.LemmaLower(token)]++
				n++
			}
			*buf = (*buf)[:0]
		}
	}
	token := string(*buf)
	if isValid(token) {
		tfm[lz.LemmaLower(token)]++
		n++
	}
	*buf = (*buf)[:0]
	return n
}

// mergeFields produces one TermFreq per unique term across the title/body
// maps. Terms only in title get FreqBody=0 (and vice versa). Body is iterated
// first because it's typically the larger map; terms also present in title
// are deleted so the second loop emits only title-only terms.
func mergeFields(tfTitle, tfBody map[string]uint32) []internal.TermFreq {
	out := make([]internal.TermFreq, 0, len(tfBody)+len(tfTitle))
	for term, fb := range tfBody {
		ft := tfTitle[term]
		out = append(out, internal.TermFreq{Term: term, FreqTitle: ft, FreqBody: fb})
		if ft != 0 {
			delete(tfTitle, term)
		}
	}
	for term, ft := range tfTitle {
		out = append(out, internal.TermFreq{Term: term, FreqTitle: ft})
	}
	return out
}

func worker(id int, Docs chan internal.Doc, debug bool, proc_pg chan internal.ProcessedDoc, lz *golem.Lemmatizer) {
	if debug {
		log.Printf("worker %d processing Doc", id)
	}

	buf := make([]byte, 0, 64)

	for Doc := range Docs {
		if debug {
			log.Printf("processing Doc %s(%d)", Doc.Title, Doc.ID)
		}

		// Two maps keep title and body counts cleanly separable; merged once
		// per doc into the per-field TermFreq slice the indexer expects.
		tfTitle := make(map[string]uint32, 16)
		tfBody := make(map[string]uint32, 100)

		titleLen := processText(Doc.Title, tfTitle, &buf, lz)
		bodyLen := processText(preprocessor.Strip(Doc.Revision.Text), tfBody, &buf, lz)

		proc_pg <- internal.ProcessedDoc{
			ID:       Doc.ID,
			TitleLen: titleLen,
			BodyLen:  bodyLen,
			Terms:    mergeFields(tfTitle, tfBody),
		}
	}
}

func Tokenize(workers int, file string, proc_pgPool chan internal.ProcessedDoc) {

	lastIndexedDocID := storage.ReadCheckPoint()

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	bz2R, err := pbzip2.NewReader(f)
	if err != nil {
		panic(err)
	}
	xmlD := xml.NewDecoder(bz2R)
	lemmatizer, err := golem.New(en.New())
	if err != nil {
		panic(err)
	}

	Doc_pool := make(chan internal.Doc, 5000)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			worker(i, Doc_pool, false, proc_pgPool, lemmatizer)
		})
	}

	for {
		tok, err := xmlD.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("err reading xml token : %v", err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		if start.Name.Local == "page" {
			var p internal.Doc
			if err := xmlD.DecodeElement(&p, &start); err != nil {
				panic(err)
			}
			if p.NS != 0 {
				continue
			}

			if uint64(p.ID) < lastIndexedDocID {
				continue
			}

			// buffered channel creates automatic backpressure and pauses the stream
			// until Doc pool is free
			Doc_pool <- p
		}
	}
	close(Doc_pool)
	wg.Wait()
	close(proc_pgPool)
}
