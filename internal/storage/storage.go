package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"slices"

	"github.com/skshmgpt/srch/internal"
)

func WriteCheckpoint(lastIndexedDocID int) {
	tmp := "indexer.chkpt.tmp"
	final := "indexer.chkpt"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	if err := binary.Write(f, binary.LittleEndian, uint64(lastIndexedDocID)); err != nil {
		f.Close()
		panic(err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		panic(err)
	}

	if err := f.Close(); err != nil {
		panic(err)
	}

	if err := os.Rename(tmp, final); err != nil {
		panic(err)
	}
}

func ReadCheckPoint() uint64 {
	f, err := os.Open("indexer.chkpt")
	if err != nil {
		return 0
	}
	defer f.Close()

	var lastIndexedDocID uint64
	if err := binary.Read(f, binary.LittleEndian, &lastIndexedDocID); err != nil {
		return 0
	}

	return lastIndexedDocID
}

func WriteIdxPart(idx *internal.Index, partIdx int, outdir string) (*internal.IdxSegment, error) {
	file_name := fmt.Sprintf("idx_part_%d", partIdx)
	file_path := path.Join(outdir, file_name)

	f, err := os.OpenFile(file_path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return &internal.IdxSegment{}, err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20) // 1MB buffer

	sortedTerms := make([]string, 0, len(idx.Df))
	for term := range idx.Df {
		sortedTerms = append(sortedTerms, term)
	}

	slices.Sort(sortedTerms)

	// Header layout:
	//   DocCount (u64) | TotalDoclen (u64) | TotalTitleLen (u64) | TotalBodyLen (u64)
	//   | numTerms (u32) | dictStart placeholder (u64)
	const termDictOffsetPos int64 = 8 + 8 + 8 + 8 + 4
	const headerSize uint64 = uint64(termDictOffsetPos) + 8

	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:8], idx.DocCount)
	bw.Write(scratch[:8])

	binary.LittleEndian.PutUint64(scratch[:8], idx.TotalDoclen)
	bw.Write(scratch[:8])

	binary.LittleEndian.PutUint64(scratch[:8], idx.TotalTitleLen)
	bw.Write(scratch[:8])

	binary.LittleEndian.PutUint64(scratch[:8], idx.TotalBodyLen)
	bw.Write(scratch[:8])

	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(sortedTerms)))
	bw.Write(scratch[:4])

	binary.LittleEndian.PutUint64(scratch[:8], 0)
	bw.Write(scratch[:8]) // placeholder for term dict start, patched after posting list is written

	pos := headerSize
	// posting list start index for each term
	offset := make(map[string]uint64, len(sortedTerms))
	postingLen := make(map[string]uint32, len(sortedTerms))

	// Per-term posting block layout:
	//   docLen (u32) | docDeltas (varints)
	//   | titleLen (u32) | titleFreqs (varints)
	//   | bodyLen  (u32) | bodyFreqs  (varints)
	var docBuf, titleBuf, bodyBuf []byte
	var varBuf [binary.MaxVarintLen64]byte
	for _, term := range sortedTerms {
		pl := idx.InvIdx[term]

		docBuf = docBuf[:0]
		titleBuf = titleBuf[:0]
		bodyBuf = bodyBuf[:0]

		prev := uint32(0)
		for _, id := range pl.DocIDS {
			delta := id - prev
			prev = id
			n := binary.PutUvarint(varBuf[:], uint64(delta))
			docBuf = append(docBuf, varBuf[:n]...)
		}
		for _, f := range pl.FreqsTitle {
			n := binary.PutUvarint(varBuf[:], uint64(f))
			titleBuf = append(titleBuf, varBuf[:n]...)
		}
		for _, f := range pl.FreqsBody {
			n := binary.PutUvarint(varBuf[:], uint64(f))
			bodyBuf = append(bodyBuf, varBuf[:n]...)
		}

		offset[term] = pos

		plen := uint32(4 + len(docBuf) + 4 + len(titleBuf) + 4 + len(bodyBuf))
		postingLen[term] = plen

		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(docBuf)))
		bw.Write(scratch[:4])
		bw.Write(docBuf)
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(titleBuf)))
		bw.Write(scratch[:4])
		bw.Write(titleBuf)
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(bodyBuf)))
		bw.Write(scratch[:4])
		bw.Write(bodyBuf)

		pos += uint64(plen)
	}
	dictStart := pos

	// term dictionary : termLen | termBytes | df | offset | postinglen
	for _, term := range sortedTerms {
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(term)))
		bw.Write(scratch[:4])
		bw.WriteString(term)
		binary.LittleEndian.PutUint32(scratch[:4], idx.Df[term])
		bw.Write(scratch[:4])
		binary.LittleEndian.PutUint64(scratch[:8], offset[term])
		bw.Write(scratch[:8])
		binary.LittleEndian.PutUint32(scratch[:4], postingLen[term]) // for reading the full list block without scanning
		bw.Write(scratch[:4])
	}

	if err := bw.Flush(); err != nil {
		return &internal.IdxSegment{}, err
	}

	if _, err := f.Seek(termDictOffsetPos, io.SeekStart); err != nil {
		return &internal.IdxSegment{}, err
	}

	binary.LittleEndian.PutUint64(scratch[:8], dictStart)
	if _, err := f.Write(scratch[:8]); err != nil {
		return &internal.IdxSegment{}, err
	}

	if err := f.Sync(); err != nil {
		return &internal.IdxSegment{}, err
	}

	return &internal.IdxSegment{
		File:          file_path,
		Docs:          idx.DocCount,
		TotalDocLen:   idx.TotalDoclen,
		TotalTitleLen: idx.TotalTitleLen,
		TotalBodyLen:  idx.TotalBodyLen,
	}, nil
}

// TermEntry is the per-term metadata kept in memory after loading a part's
// dictionary. Used to fetch and score the posting list at query time.
type TermEntry struct {
	Df         uint32 // local to this part; sum across parts for global IDF
	Offset     uint64 // byte offset of this term's posting block in the part file
	PostingLen uint32 // total bytes of the posting block
}

// Term is a single (string, meta) pair in the loaded term dictionary.
// Stored as a slice (sorted by Literal) so callers can binary-search; this
// is ~3x more compact than map[string]TermEntry for large vocabularies.
type Term struct {
	Literal string
	Meta    TermEntry
}

// IdxPart is a loaded index part: in-memory term dict + open file handle for
// posting list random access via ReadAt. Caller owns the lifetime of F and
// must Close it when done.
type IdxPart struct {
	File          string
	F             *os.File
	DocCount      uint64
	TotalDocLen   uint64
	TotalTitleLen uint64
	TotalBodyLen  uint64
	Terms         []Term
}

// ReadIdxTermDict loads a part's header + term dictionary into memory and
// keeps the underlying file open for posting list random access via ReadAt.
// Caller owns the returned IdxPart.F and must Close it when done.
func ReadIdxTermDict(filePath string) (*IdxPart, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	// NOTE: do NOT defer f.Close() — f is handed to the caller in IdxPart.

	var scratch [8]byte

	readU32At := func(off int64) (uint32, error) {
		if _, err := f.ReadAt(scratch[:4], off); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(scratch[:4]), nil
	}
	readU64At := func(off int64) (uint64, error) {
		if _, err := f.ReadAt(scratch[:8], off); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(scratch[:8]), nil
	}

	// Header layout (must match WriteIdxPart):
	//   0   DocCount      u64
	//   8   TotalDoclen   u64
	//   16  TotalTitleLen u64
	//   24  TotalBodyLen  u64
	//   32  numTerms      u32
	//   36  dictStart     u64
	docCount, err := readU64At(0)
	if err != nil {
		f.Close()
		return nil, err
	}
	totalDocLen, _ := readU64At(8)
	totalTitleLen, _ := readU64At(16)
	totalBodyLen, _ := readU64At(24)
	numTerms, _ := readU32At(32)
	dictStart, err := readU64At(36)
	if err != nil {
		f.Close()
		return nil, err
	}

	if _, err := f.Seek(int64(dictStart), io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	br := bufio.NewReaderSize(f, 1<<20)

	readU32 := func() (uint32, error) {
		if _, err := io.ReadFull(br, scratch[:4]); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(scratch[:4]), nil
	}
	readU64 := func() (uint64, error) {
		if _, err := io.ReadFull(br, scratch[:8]); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(scratch[:8]), nil
	}

	terms := make([]Term, numTerms)
	for i := uint32(0); i < numTerms; i++ {
		termLen, err := readU32()
		if err != nil {
			f.Close()
			return nil, err
		}
		termBytes := make([]byte, termLen)
		if _, err := io.ReadFull(br, termBytes); err != nil {
			f.Close()
			return nil, err
		}
		df, _ := readU32()
		offset, _ := readU64()
		postingLen, _ := readU32()

		terms[i] = Term{
			Literal: string(termBytes),
			Meta: TermEntry{
				Df:         df,
				Offset:     offset,
				PostingLen: postingLen,
			},
		}
	}

	return &IdxPart{
		File:          filePath,
		F:             f,
		DocCount:      docCount,
		TotalDocLen:   totalDocLen,
		TotalTitleLen: totalTitleLen,
		TotalBodyLen:  totalBodyLen,
		Terms:         terms,
	}, nil
}

func (ip *IdxPart) ReadTermPostingList(off int64, postingLen uint64, df uint32) (*internal.PostingList, error) {
	buf := make([]byte, postingLen)
	if _, err := ip.F.ReadAt(buf, off); err != nil {
		return nil, err
	}

	pos := 0
	readSection := func() ([]uint32, error) {
		if pos+4 > len(buf) {
			return nil, fmt.Errorf("posting truncated")
		}
		n := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		pos += 4
		end := pos + n
		if end > len(buf) {
			return nil, fmt.Errorf("posting section overruns block")
		}
		out := make([]uint32, 0, df)
		for pos < end {
			v, k := binary.Uvarint(buf[pos:end])
			if k <= 0 {
				return nil, fmt.Errorf("bad varint")
			}
			out = append(out, uint32(v))
			pos += k
		}
		return out, nil
	}

	docDeltas, err := readSection()
	if err != nil {
		return nil, err
	}
	// delta decode docIDS
	var prev uint32
	for i, d := range docDeltas {
		prev += d
		docDeltas[i] = prev
	}

	freqsTitle, err := readSection()
	if err != nil {
		return nil, err
	}
	freqsBody, err := readSection()
	if err != nil {
		return nil, err
	}

	return &internal.PostingList{
		DocIDS:     docDeltas,
		FreqsTitle: freqsTitle,
		FreqsBody:  freqsBody,
	}, nil
}
