package main

import (
	"fmt"
	"os"

	v1 "github.com/grafana/loki/v3/pkg/storage/bloom/v1"
	"github.com/grafana/loki/v3/pkg/util/mempool"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] == "-h" {
		fmt.Println("Usage: go run main.go BLOCK_DIRECTORY")
		os.Exit(2)
	}

	path := os.Args[1]
	fmt.Printf("Block:    %s\n", path)

	r := v1.NewDirectoryBlockReader(path)
	b := v1.NewBlock(r, v1.NewMetrics(nil))
	q := v1.NewBlockQuerier(b, &mempool.SimpleHeapAllocator{}, v1.DefaultMaxPageSize)

	md, err := q.Metadata()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Checksum: 0x%x\n", md.Checksum)
	fmt.Printf("Series:   %+v\n", md.Series)
	fmt.Printf("Options:  %+v\n", md.Options)

	for q.Next() {
		swb := q.At()
		fmt.Printf("%s (%+v) %v\n", swb.Series.Fingerprint, swb.Series.Chunks, swb.Meta.Fields.Items())
	}
	if q.Err() != nil {
		fmt.Printf("error: %s\n", q.Err())
	}
}
