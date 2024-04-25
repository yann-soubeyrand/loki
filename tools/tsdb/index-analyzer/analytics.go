package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/index"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb"
	tsdb_index "github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb/index"
)

type stats struct {
	series model.Fingerprint
	table  string
	found  bool
	labels string
	chunks uint64
	lines  uint64
	B      uint64
}

func analyze(indexShipper indexshipper.IndexShipper, tableName string, tenants []string) (error, []stats) {
	seriesRaw := strings.Split(os.Getenv("SERIES"), ",")
	seriesToLookup := make(map[model.Fingerprint]stats, len(seriesRaw))
	for _, s := range seriesRaw {
		si, err := strconv.ParseUint(s, 16, 64)
		if err != nil {
			return err, nil
		}

		seriesToLookup[model.Fingerprint(si)] = stats{
			series: model.Fingerprint(si),
			table:  tableName,
		}
	}

	var (
	//series int
	//chunks int
	//seriesRes          []tsdb.Series
	//chunkRes           []tsdb.ChunkRef
	//maxChunksPerSeries int
	//seriesOver1kChunks int
	)
	for _, tenant := range tenants {
		if tenant != "29" {
			fmt.Printf("skipping tenant %s\n", tenant)
			continue
		}

		fmt.Printf("analyzing tenant %s\n", tenant)
		err := indexShipper.ForEach(
			context.Background(),
			tableName,
			tenant,
			index.ForEachIndexCallback(func(isMultiTenantIndex bool, idx index.Index) error {
				if isMultiTenantIndex {
					return nil
				}

				casted := idx.(*tsdb.TSDBFile)
				//seriesRes = seriesRes[:0]
				//chunkRes = chunkRes[:0]
				//
				//res, err := casted.Series(
				//	context.Background(),
				//	tenant,
				//	model.Earliest,
				//	model.Latest,
				//	seriesRes, nil,
				//	labels.MustNewMatcher(labels.MatchEqual, "", ""),
				//)
				//
				//if err != nil {
				//	return err
				//}
				//
				//series += len(res)
				//
				//chunkRes, err := casted.GetChunkRefs(
				//	context.Background(),
				//	tenant,
				//	model.Earliest,
				//	model.Latest,
				//	chunkRes, nil,
				//	labels.MustNewMatcher(labels.MatchEqual, "", ""),
				//)
				//
				//if err != nil {
				//	return err
				//}
				//
				//chunks += len(chunkRes)

				err := casted.Index.(*tsdb.TSDBIndex).ForSeries(
					context.Background(),
					"", nil,
					model.Earliest,
					model.Latest,
					func(ls labels.Labels, fp model.Fingerprint, chks []tsdb_index.ChunkMeta) (stop bool) {
						_, found := seriesToLookup[fp]
						if !found {
							return false
						}

						hex := strconv.FormatUint(uint64(fp), 16)

						var lines, kbytes uint64
						for _, chk := range chks {
							lines += uint64(chk.Entries)
							kbytes += uint64(chk.KB)
						}

						fmt.Printf("found series (%s) %s\n", hex, ls.String())
						fmt.Printf(" \t%d chunks, %d lines, %d KB (%f GB)\n", len(chks), lines, kbytes, float64(kbytes)/1024/1024)

						seriesToLookup[fp] = stats{
							series: fp,
							table:  tableName,
							found:  true,
							labels: ls.String(),
							chunks: uint64(len(chks)),
							lines:  lines,
							B:      kbytes * 1024,
						}

						//if len(chks) > maxChunksPerSeries {
						//	maxChunksPerSeries = len(chks)
						//	if len(chks) > 1000 {
						//		seriesOver1kChunks++
						//	}
						//}
						return false
					},
					labels.MustNewMatcher(labels.MatchEqual, "", ""),
				)

				if err != nil {
					return err
				}

				return nil
			}),
		)

		if err != nil {
			return err, nil
		}
	}

	//allSeries := make([]model.Fingerprint, 0, len(seriesToLookup))
	//for fp := range seriesToLookup {
	//	allSeries = append(allSeries, fp)
	//}
	//// sort
	//slices.Sort(allSeries)
	//
	//fmt.Printf("series, chunks, bytes, lines, labels\n")
	//
	//for _, fp := range allSeries {
	//	stats := seriesToLookup[fp]
	//	if !stats.found {
	//		fmt.Printf("%s, %s, na, na, na, na\n\n", fp, tableName)
	//		continue
	//	}
	//
	//	fmt.Printf("%s, %s, %d, %d, %d, `%s`\n\n", fp, tableName, stats.chunks, stats.B, stats.lines, stats.labels)
	//}

	output := make([]stats, 0, len(seriesToLookup))
	for _, v := range seriesToLookup {
		output = append(output, v)
	}

	//fmt.Printf("analyzed %d series and %d chunks for an average of %f chunks per series. max chunks/series was %d. number of series with over 1k chunks: %d\n", series, chunks, float64(chunks)/float64(series), maxChunksPerSeries, seriesOver1kChunks)

	return nil, output
}
