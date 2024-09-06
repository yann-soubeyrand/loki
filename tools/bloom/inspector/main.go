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
	q := v1.NewBlockQuerier(b, &mempool.SimpleHeapAllocator{}, 256<<20)
	itr := q.Iter()

	md, err := q.Metadata()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Checksum: 0x%x\n", md.Checksum)
	fmt.Printf("Series:   %+v\n", md.Series)
	fmt.Printf("Options:  %+v\n", md.Options)

	fmt.Println("-----------------------------")

	count := 0
	for itr.Next() {
		swb := itr.At()
		series := swb.Series
		meta := series.Meta
		p := 0
		for swb.Blooms.Next() {
			bloom := swb.Blooms.At()
			fmt.Printf(
				"fp=%s page=%d chunks=%d fields=%v size=%vB fill=%v count=%v\n",
				series.Fingerprint,
				p,
				series.Chunks.Len(),
				meta.Fields.Items(),
				bloom.Capacity()/8,
				bloom.FillRatio(),
				bloom.Count(),
			)
			p++
		}
		count++
	}
	fmt.Printf("Stream count: %4d\n", count)

	if q.Err() != nil {
		fmt.Printf("error: %s\n", q.Err())
	}
}
