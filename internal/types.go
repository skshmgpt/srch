package internal

type Doc struct {
	Title    string `xml:"title"`
	NS       int    `xml:"ns"`
	ID       uint32 `xml:"id"`
	Revision struct {
		Text string `xml:"text"`
	} `xml:"revision"`
}

type ProcessedDoc struct {
	ID       uint32
	TitleLen uint32
	BodyLen  uint32
	Terms    []TermFreq
}

// TermFreq carries the term's frequency split by field. A term that appears
// only in the body has FreqTitle=0 and vice versa.
type TermFreq struct {
	Term      string
	FreqTitle uint32
	FreqBody  uint32
}

type PostingList struct {
	DocIDS     []uint32
	FreqsTitle []uint32
	FreqsBody  []uint32
}

type Index struct {
	InvIdx        map[string]*PostingList
	Df            map[string]uint32
	DocLen        map[uint32]uint32 // total (title+body) length per doc
	DocCount      uint64
	TotalDoclen   uint64 // sum of title+body across all docs
	TotalTitleLen uint64 // for avgTitleLen at query time
	TotalBodyLen  uint64 // for avgBodyLen at query time
}

type IdxSegment struct {
	File          string
	Docs          uint64
	TotalDocLen   uint64
	TotalTitleLen uint64
	TotalBodyLen  uint64
}

type Manifest struct {
	IdxParts      []IdxSegment
	Docs          uint64
	TotalDocLen   uint64
	TotalTitleLen uint64
	TotalBodyLen  uint64
}
