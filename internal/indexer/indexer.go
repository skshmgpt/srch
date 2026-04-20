package indexer

import (
	"encoding/binary"
	"sort"

	"github.com/skshmgpt/srch/internal/tokenizer"
)

// read from a pool of processed Docs
// build posting
// write to an in-memory map
//

type PostingList struct {
	PageIDS []byte
	Freqs   []byte
}

type Index struct {
	InvIdx       map[string]*PostingList
	Df           map[string]int
	PageCount    int
	PageLen      map[int]int
	TotalPageLen int
}

type Posting struct {
	pageID uint32
	freq   uint32
}

func BuildIndex(proc_DocPool <-chan tokenizer.ProcessedPage) *Index {

	index := Index{
		InvIdx:  make(map[string]*PostingList),
		Df:      make(map[string]int),
		PageLen: make(map[int]int),
	}

	tempIdx := make(map[string][]Posting)

	for pp := range proc_DocPool {
		tf := 0
		for term, freq := range pp.FreqMap {
			tempIdx[term] = append(tempIdx[term], Posting{
				pageID: uint32(pp.ID),
				freq:   uint32(freq),
			})
			index.Df[term]++
			tf += freq
		}
		index.PageCount++
		index.PageLen[pp.ID] = tf
		index.TotalPageLen += tf
	}

	for term, pArr := range tempIdx {
		sort.Slice(pArr, func(i, j int) bool {
			return pArr[i].pageID < pArr[j].pageID
		})

		for i := len(pArr) - 1; i > 0; i-- {
			pArr[i].pageID = pArr[i].pageID - pArr[i-1].pageID
		}

		compressedPageIDS := make([]byte, 0, len(pArr)*2)
		compressedFreqs := make([]byte, 0, len(pArr)*2)
		buf := make([]byte, binary.MaxVarintLen64)

		for _, d := range pArr {
			n := binary.PutUvarint(buf, uint64(d.pageID))
			compressedPageIDS = append(compressedPageIDS, buf[:n]...)
			n = binary.PutUvarint(buf, uint64(d.freq))
			compressedFreqs = append(compressedFreqs, buf[:n]...)
		}

		index.InvIdx[term] = &PostingList{
			PageIDS: compressedPageIDS,
			Freqs:   compressedFreqs,
		}

	}
	clear(tempIdx)

	return &index

}
