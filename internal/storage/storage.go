package storage

import (
	"encoding/binary"
	"io"
	"os"
	"sort"

	"github.com/skshmgpt/srch/internal/indexer"
)

// hardcoded atp
const VERSION = 1

func StoreIndex(index *indexer.Index) {
	f, err := os.OpenFile("tmp.idx", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// write header
	binary.Write(f, binary.LittleEndian, []byte("SRCH"))
	binary.Write(f, binary.LittleEndian, uint32(VERSION))
	binary.Write(f, binary.LittleEndian, uint32(index.PageCount))
	binary.Write(f, binary.LittleEndian, uint32(index.TotalPageLen))

	// reserve space for termTableOffset
	termTableOffsetPos, _ := f.Seek(0, io.SeekCurrent)
	binary.Write(f, binary.LittleEndian, uint64(0))

	// reserve space for postingsListOffset
	postingsOffsetPos, _ := f.Seek(0, io.SeekCurrent)
	binary.Write(f, binary.LittleEndian, uint64(0))

	type TermMeta struct {
		Offset uint64
		Length uint32
		DF     uint32
	}

	// used for writing term table
	termMeta := make(map[string]TermMeta)

	terms := make([]string, 0, len(index.InvIdx))
	for term := range index.InvIdx {
		terms = append(terms, term)
	}
	sort.Strings(terms)

	postingsOffset, _ := f.Seek(0, io.SeekCurrent)
	f.Seek(postingsOffsetPos, io.SeekStart)
	binary.Write(f, binary.LittleEndian, uint64(postingsOffset))
	f.Seek(postingsOffset, io.SeekStart)
	// postings
	for _, term := range terms {
		pl := index.InvIdx[term]
		offset, _ := f.Seek(0, io.SeekCurrent)

		binary.Write(f, binary.LittleEndian, uint32(len(pl.PageIDS)))
		binary.Write(f, binary.LittleEndian, pl.PageIDS)

		binary.Write(f, binary.LittleEndian, uint32(len(pl.Freqs)))
		binary.Write(f, binary.LittleEndian, pl.Freqs)

		end, _ := f.Seek(0, io.SeekCurrent)
		termMeta[term] = TermMeta{
			Offset: uint64(offset - postingsOffset),
			Length: uint32(end - offset),
			DF:     uint32(index.Df[term]),
		}
	}

	termTableOffset, _ := f.Seek(0, io.SeekCurrent)
	f.Seek(termTableOffsetPos, io.SeekStart)
	binary.Write(f, binary.LittleEndian, uint64(termTableOffset))
	f.Seek(termTableOffset, io.SeekStart)
	// term table
	binary.Write(f, binary.LittleEndian, uint32(len(terms)))
	for _, term := range terms {
		binary.Write(f, binary.LittleEndian, uint16(len(term)))
		f.Write([]byte(term))

		tm := termMeta[term]

		binary.Write(f, binary.LittleEndian, tm.DF)
		binary.Write(f, binary.LittleEndian, tm.Offset)
		binary.Write(f, binary.LittleEndian, tm.Length)
	}

}

type termMeta struct {
	DF            uint32
	PostingOffset uint64
	PostingLen    uint32
}

type MetaInvIdx struct {
	DocCount          int
	TotalDocLen       int
	PostingListOffset uint64
	TermTable         map[string]termMeta
}

func ReadIndex() *MetaInvIdx {
	f, err := os.Open("tmp.idx")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// read header
	var magic [4]byte
	binary.Read(f, binary.LittleEndian, &magic)

	var version uint32
	binary.Read(f, binary.LittleEndian, &version)

	var DocCount uint32
	binary.Read(f, binary.LittleEndian, &DocCount)

	var totalDocLen uint32
	binary.Read(f, binary.LittleEndian, &totalDocLen)

	var termTableOffset uint64
	binary.Read(f, binary.LittleEndian, &termTableOffset)

	var postingListOffset uint64
	binary.Read(f, binary.LittleEndian, &postingListOffset)

	// read term table
	f.Seek(int64(termTableOffset), io.SeekStart)
	var terms uint32
	binary.Read(f, binary.LittleEndian, &terms)

	termTable := make(map[string]termMeta)

	for i := uint32(0); i < terms; i++ {
		var l uint16
		binary.Read(f, binary.LittleEndian, &l)
		term := make([]byte, l)
		f.Read(term)

		tm := termMeta{}

		binary.Read(f, binary.LittleEndian, &tm.DF)
		binary.Read(f, binary.LittleEndian, &tm.PostingOffset)
		binary.Read(f, binary.LittleEndian, &tm.PostingLen)
		termTable[string(term)] = tm
	}

	return &MetaInvIdx{
		DocCount:          int(DocCount),
		TotalDocLen:       int(totalDocLen),
		TermTable:         termTable,
		PostingListOffset: postingListOffset,
	}

}
