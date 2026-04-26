package main

import (
	"log"
	"sort"

	"github.com/skshmgpt/srch/internal/storage"
)

func main() {
	ip, err := storage.ReadIdxTermDict("idx_part_1")
	if err != nil {
		panic(err)
	}

	const term = "anthropic"

	i := sort.Search(len(ip.Terms), func(i int) bool {
		return ip.Terms[i].Literal >= term
	})
	if i == len(ip.Terms) || ip.Terms[i].Literal != term {
		log.Fatalf("term %q not found", term)
	}
	entry := ip.Terms[i].Meta

	postings, err := ip.ReadTermPostingList(int64(entry.Offset), uint64(entry.PostingLen), entry.Df)
	if err != nil {
		panic(err)
	}
	for _, id := range postings.DocIDS {
		log.Println(id)
	}
}
