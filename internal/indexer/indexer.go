package indexer

import (
	"encoding/binary"
	"sort"

	"github.com/skshmgpt/srch/internal/tokenizer"
)

// read from a pool of processed pages
// build posting
// write to an in-memory map
//

type PostingList struct {
	DocIDS []byte
	Freqs  []uint32
}

type Index struct {
	InvIdx       map[string]*PostingList
	Df           map[string]int
	PageCount    int
	PageLen      map[int]int
	TotalpageLen int
}

type Posting struct {
	docID uint32
	freq  uint32
}

func BuildIndex(proc_pagePool <-chan tokenizer.ProcessedPage) *Index {

	index := Index{
		InvIdx:  make(map[string]*PostingList),
		Df:      make(map[string]int),
		PageLen: make(map[int]int),
	}

	tempIdx := make(map[string][]Posting)

	for pp := range proc_pagePool {
		tf := 0
		for term, freq := range pp.FreqMap {
			tempIdx[term] = append(tempIdx[term], Posting{
				docID: uint32(pp.ID),
				freq:  uint32(freq),
			})
			index.Df[term]++
			tf += freq
		}
		index.PageCount++
		index.PageLen[pp.ID] = tf
		index.TotalpageLen += tf
	}

	for term, pArr := range tempIdx {
		sort.Slice(pArr, func(i, j int) bool {
			return pArr[i].docID < pArr[j].docID
		})

		for i := len(pArr) - 1; i > 0; i-- {
			pArr[i].docID = pArr[i].docID - pArr[i-1].docID
		}

		compressedDocIDS := make([]byte, 0, len(pArr)*2)
		freqs := make([]uint32, len(pArr))
		buf := make([]byte, binary.MaxVarintLen64)

		for i, d := range pArr {
			n := binary.PutUvarint(buf, uint64(d.docID))
			compressedDocIDS = append(compressedDocIDS, buf[:n]...)
			freqs[i] = pArr[i].freq
		}

		index.InvIdx[term] = &PostingList{
			DocIDS: compressedDocIDS,
			Freqs:  freqs,
		}
		clear(compressedDocIDS)
		clear(freqs)
	}

	return &index

}
