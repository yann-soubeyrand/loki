package v1

import (
	"bytes"
	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBloomBlock_BloomPageDecoder_PageSize(t *testing.T) {
	const bloomsToBuild = 100

	tests := []struct {
		name           string
		maxPageSize    int
		expectedBlooms int
	}{
		{
			name:           "below max page size",
			maxPageSize:    DefaultMaxPageSize,
			expectedBlooms: bloomsToBuild,
		},
		{
			name:           "above max page size",
			maxPageSize:    10,
			expectedBlooms: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blockOpts := BlockOptions{
				Schema: Schema{
					version:  DefaultSchemaVersion,
					encoding: chunkenc.EncSnappy,
				},
				SeriesPageSize: 100,
				BloomPageSize:  10 << 10,
			}

			numSeries := bloomsToBuild
			numKeysPerSeries := 100
			data, _ := MkBasicSeriesWithBlooms(numSeries, numKeysPerSeries, 0, 0xffff, 0, 10000)

			// references for linking in memory reader+writer
			indexBuf := bytes.NewBuffer(nil)
			bloomsBuf := bytes.NewBuffer(nil)

			writer := NewMemoryBlockWriter(indexBuf, bloomsBuf)
			reader := NewByteReader(indexBuf, bloomsBuf)

			builder, err := NewBlockBuilder(
				blockOpts,
				writer,
			)

			itr := NewSliceIter[SeriesWithBloom](data)
			_, err = builder.BuildFrom(itr)
			require.Nil(t, err)

			block := NewBlock(reader, NewMetrics(nil))
			querier := NewBlockQuerier(block, false, tc.maxPageSize)

			var blooms int
			for querier.Next() {
				blooms++
			}

			require.Equal(t, tc.expectedBlooms, blooms)
		})
	}
}
