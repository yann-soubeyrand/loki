package main

import (
	"context"
	"fmt"
	"golang.org/x/exp/slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/index"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb"
	tsdb_index "github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb/index"
)

type Stat struct {
	chunk     tsdb_index.ChunkMeta
	labels    labels.Labels
	daysDelta int64
}

func analyze(indexShipper indexshipper.IndexShipper, tableName string, tenants []string) error {

	var (
		series             int
		chunks             int
		seriesRes          []tsdb.Series
		chunkRes           []tsdb.ChunkRef
		maxChunksPerSeries int
		seriesOver1kChunks int

		chunksBeforeDay []Stat
	)
	for _, tenant := range tenants {
		if tenant != "29" {
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
				seriesRes = seriesRes[:0]
				chunkRes = chunkRes[:0]

				res, err := casted.Series(
					context.Background(),
					tenant,
					model.Earliest,
					model.Latest,
					seriesRes, nil,
					labels.MustNewMatcher(labels.MatchEqual, "", ""),
				)

				if err != nil {
					return err
				}

				series += len(res)

				chunkRes, err := casted.GetChunkRefs(
					context.Background(),
					tenant,
					model.Earliest,
					model.Latest,
					chunkRes, nil,
					labels.MustNewMatcher(labels.MatchEqual, "", ""),
				)

				if err != nil {
					return err
				}

				chunks += len(chunkRes)

				//fileName := casted.Name()
				//tsdbMeta, ok := tsdb.ParseSingleTenantTSDBPath(fileName)
				//if !ok {
				//	panic("Could not parse tsdb file name")
				//}

				tableNameParts := strings.Split(tableName, "_")
				tableDay, err := strconv.Atoi(tableNameParts[len(tableNameParts)-1])
				if err != nil {
					panic(err)
				}

				err = casted.Index.(*tsdb.TSDBIndex).ForSeries(
					context.Background(),
					"", nil,
					model.Earliest,
					model.Latest,
					func(ls labels.Labels, fp model.Fingerprint, chks []tsdb_index.ChunkMeta) (stop bool) {
						for _, chunk := range chks {
							chunkStartDay := int64(chunk.From() / (24 * 60 * 60 * 1000))

							if chunkStartDay < int64(tableDay) {
								chunksBeforeDay = append(chunksBeforeDay, Stat{
									chunk:     chunk,
									labels:    ls,
									daysDelta: int64(tableDay) - chunkStartDay,
								})
							}
						}

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
			return err
		}
	}

	fmt.Printf("analyzed %d series and %d chunks for an average of %f chunks per series. max chunks/series was %d. number of series with over 1k chunks: %d\n", series, chunks, float64(chunks)/float64(series), maxChunksPerSeries, seriesOver1kChunks)

	fmt.Printf("%d chunks before the daytable\n", len(chunksBeforeDay))

	type chunkStats struct {
		lbs      labels.Labels
		duration time.Duration
		sizeKB   uint32
		entries  uint32
	}

	type dayStats struct {
		numberOfChunks int
		chunkStats     []chunkStats
	}

	daysDistirbution := make(map[int64]dayStats)

	for _, stat := range chunksBeforeDay {
		curr := daysDistirbution[stat.daysDelta]

		daysDistirbution[stat.daysDelta] = dayStats{
			numberOfChunks: curr.numberOfChunks + 1,
			chunkStats: append(curr.chunkStats, chunkStats{
				lbs:      stat.labels,
				duration: stat.chunk.Through().Sub(stat.chunk.From()),
				sizeKB:   stat.chunk.KB,
				entries:  stat.chunk.Entries,
			}),
		}
	}

	for days, stat := range daysDistirbution {
		stats := stat.chunkStats
		slices.SortFunc(stats, func(i, j chunkStats) int {
			return int(i.duration - j.duration)
		})

		fmt.Printf("delta: %d\t-> chunks: %d\n", days, stat.numberOfChunks)
		fmt.Printf("\t\t top 3 chunk.Through - chunk.From diff:\n")
		for i := 1; i <= 3 && len(stats)-i >= 0; i++ {
			fmt.Printf("\t\t\t #%d\n", i)
			fmt.Printf("\t\t\t\t labels: %s\n", stats[len(stats)-i].lbs.String())
			fmt.Printf("\t\t\t\t duration: %s\n", stats[len(stats)-i].duration)
			fmt.Printf("\t\t\t\t sizeKB: %d\n", stats[len(stats)-i].sizeKB)
			fmt.Printf("\t\t\t\t entries: %d\n", stats[len(stats)-i].entries)
		}
	}

	return nil
}
