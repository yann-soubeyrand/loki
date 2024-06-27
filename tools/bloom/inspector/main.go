package main

import (
	"fmt"
	"os"

	v1 "github.com/grafana/loki/v3/pkg/storage/bloom/v1"
	"github.com/grafana/loki/v3/pkg/util/mempool"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go BLOCK_DIRECTORY [TEST ...]")
		os.Exit(2)
	}

	path := os.Args[1]
	fmt.Printf("Block directory: %s\n", path)

	test := os.Args[2:]
	fmt.Printf("Test: %v\n", test)

	r := v1.NewDirectoryBlockReader(path)
	b := v1.NewBlock(r, v1.NewMetrics(nil))
	q := v1.NewBlockQuerier(b, &mempool.SimpleHeapAllocator{}, v1.DefaultMaxPageSize)

	md, err := q.Metadata()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Metadata: %+v\n", md)

	itr := q.Iter()
	for itr.Next() {
		q := itr.At()
		if len(test) > 0 {
			for q.Blooms.Next() {
				bloom := q.Blooms.At()
				for i := range test {
					tokenizer := v1.NewNGramTokenizer(md.Options.Schema.NGramLen(), md.Options.Schema.NGramSkip())
					tokens := tokenizer.Tokens(test[i])
					for tokens.Next() {
						res := bloom.Test(tokens.At())
						fmt.Printf("%s => %v\n", tokens.At(), res)
					}
				}
			}
		}
		fmt.Printf("%s (%d)\n", q.Series.Fingerprint, q.Series.Chunks.Len())
	}
	if q.Err() != nil {
		fmt.Printf("error: %s\n", q.Err())
	}
}
