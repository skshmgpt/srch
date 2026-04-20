package tokenizer

import (
	"compress/bzip2"
	"encoding/xml"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"unicode"

	"github.com/aaaton/golem/v4"
	"github.com/aaaton/golem/v4/dicts/en"
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

type Page struct {
	Title    string `xml:"title"`
	NS       int    `xml:"ns"`
	ID       int    `xml:"id"`
	Revision struct {
		Text string `xml:"text"`
	} `xml:"revision"`
}

type ProcessedPage struct {
	ID      int
	FreqMap map[string]int
}

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

func processText(text string, word_wt int, tfm map[string]int, sb *strings.Builder, lz *golem.Lemmatizer) {
	for _, r := range text {
		r = unicode.ToLower(r)
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			sb.WriteRune(r)
		} else {
			// words in Doc.Title receive more freq to be used in ranking later
			token := sb.String()
			if isValid(token) {
				token = lz.Lemma(token)
				tfm[token] += word_wt
				sb.Reset()
			}
		}
	}
	// process last rune
	token := sb.String()
	if isValid(token) {
		token = lz.Lemma(token)
		tfm[token] += word_wt
	}
}

func worker(id int, pages chan Page, wg *sync.WaitGroup, debug bool, proc_pg chan ProcessedPage, lz *golem.Lemmatizer) {
	if debug {
		log.Printf("worker %d processing Doc", id)
	}
	defer wg.Done()

	sb := strings.Builder{}

	for page := range pages {
		sb.Reset()
		if debug {
			log.Printf("processing page %s(%d)", page.Title, page.ID)
		}

		termFreqMap := make(map[string]int, 100)

		pp := ProcessedPage{
			ID:      page.ID,
			FreqMap: termFreqMap,
		}

		processText(page.Title, 3, termFreqMap, &sb, lz)
		processText(page.Revision.Text, 1, termFreqMap, &sb, lz)
		proc_pg <- pp
	}
}

func Tokenize(file string, proc_pgPool chan ProcessedPage) {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	bz2R := bzip2.NewReader(f)
	xmlD := xml.NewDecoder(bz2R)
	lemmatizer, err := golem.New(en.New())
	if err != nil {
		panic(err)
	}

	n := runtime.NumCPU()

	page_pool := make(chan Page, 100)
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for id := range n * 2 {
		go worker(id, page_pool, &wg, false, proc_pgPool, lemmatizer)
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
			var p Page
			if err := xmlD.DecodeElement(&p, &start); err != nil {
				panic(err)
			}
			if p.NS != 0 {
				continue
			}

			// buffered channel creates automatic backpressure and pauses the stream
			// until page pool is free
			page_pool <- p
		}
	}
	close(page_pool)
	wg.Wait()
	close(proc_pgPool)
}
