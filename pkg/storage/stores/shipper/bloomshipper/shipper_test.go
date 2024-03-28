package bloomshipper

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	v1 "github.com/grafana/loki/pkg/storage/bloom/v1"
)

func TestBloomShipper_findBlocks(t *testing.T) {
	tests := map[string]struct {
		minFingerprint uint64
		maxFingerprint uint64
		startTimestamp model.Time
		endTimestamp   model.Time
		filtered       bool
	}{
		"expected block not to be filtered out if minFingerprint and startTimestamp are within range": {
			filtered: false,

			minFingerprint: 100,
			maxFingerprint: 220, // outside range
			startTimestamp: 300,
			endTimestamp:   401, // outside range
		},
		"expected block not to be filtered out if maxFingerprint and endTimestamp are within range": {
			filtered: false,

			minFingerprint: 50, // outside range
			maxFingerprint: 200,
			startTimestamp: 250, // outside range
			endTimestamp:   400,
		},
		"expected block to be filtered out if fingerprints are outside range": {
			filtered: true,

			minFingerprint: 50, // outside range
			maxFingerprint: 60, // outside range
			startTimestamp: 300,
			endTimestamp:   400,
		},
		"expected block to be filtered out if timestamps are outside range": {
			filtered: true,

			minFingerprint: 200,
			maxFingerprint: 100,
			startTimestamp: 401, // outside range
			endTimestamp:   500, // outside range
		},
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			ref := createBlockRef(data.minFingerprint, data.maxFingerprint, data.startTimestamp, data.endTimestamp)
			blocks := BlocksForMetas([]Meta{{Blocks: []BlockRef{ref}}}, NewInterval(300, 400), []model.Fingerprint{100, 200})
			if data.filtered {
				require.Empty(t, blocks)
				return
			}
			require.Len(t, blocks, 1)
			require.Equal(t, ref, blocks[0])
		})
	}
}

func TestBloomShipper_BlocksForMetas(t *testing.T) {
	for _, tc := range []struct {
		name         string
		metas        []Meta
		interval     Interval
		fingerprints []model.Fingerprint
		expectedRefs []BlockRef
	}{
		{
			name: "one block for all fp",
			metas: []Meta{
				{
					Blocks: []BlockRef{createBlockRef(100, 200, 300, 400)},
				},
			},
			interval:     NewInterval(300, 400),
			fingerprints: []model.Fingerprint{100, 200},
			expectedRefs: []BlockRef{createBlockRef(100, 200, 300, 400)},
		},
		{
			name: "two blocks",
			metas: []Meta{
				{
					Blocks: []BlockRef{
						createBlockRef(100, 200, 300, 400),
						createBlockRef(201, 300, 300, 400),
					},
				},
			},
			interval:     NewInterval(300, 400),
			fingerprints: []model.Fingerprint{100, 200, 300},
			expectedRefs: []BlockRef{
				createBlockRef(100, 200, 300, 400),
				createBlockRef(201, 300, 300, 400),
			},
		},
		{
			name: "two metas",
			metas: []Meta{
				{
					Blocks: []BlockRef{createBlockRef(100, 200, 300, 400)},
				},
				{
					Blocks: []BlockRef{createBlockRef(201, 300, 300, 400)},
				},
			},
			interval:     NewInterval(300, 400),
			fingerprints: []model.Fingerprint{100, 200, 300},
			expectedRefs: []BlockRef{
				createBlockRef(100, 200, 300, 400),
				createBlockRef(201, 300, 300, 400),
			},
		},
		{
			name: "two blocks same fp diff interval",
			metas: []Meta{
				{
					Blocks: []BlockRef{
						createBlockRef(100, 200, 300, 400),
						createBlockRef(100, 200, 400, 500),
					},
				},
			},
			interval:     NewInterval(300, 500),
			fingerprints: []model.Fingerprint{100, 200},
			expectedRefs: []BlockRef{
				createBlockRef(100, 200, 300, 400),
				createBlockRef(100, 200, 400, 500),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks := BlocksForMetas(tc.metas, tc.interval, tc.fingerprints)
			require.Len(t, blocks, len(tc.expectedRefs))
			require.ElementsMatch(t, tc.expectedRefs, blocks)
		})
	}
}

func TestBloomShipper_ForEach(t *testing.T) {
	blockRefs := make([]BlockRef, 0, 3)

	store, _, _ := newMockBloomStore(t)
	for i := 0; i < len(blockRefs); i++ {
		block, err := createBlockInStorage(t, store, "tenant", model.Time(i*24*int(time.Hour)), 0x0000, 0x00ff)
		require.NoError(t, err)
		blockRefs = append(blockRefs, block.BlockRef)
	}
	shipper := NewShipper(store)

	var count int
	err := shipper.ForEach(context.Background(), blockRefs, func(_ *v1.BlockQuerier, _ v1.FingerprintBounds) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(blockRefs), count)

	// check that the BlockDirectory ref counter is 0
	// for i := 0; i < len(blockRefs); i++ {
	// 	s := store.stores[0]
	// 	key := s.Block(blockRefs[i]).Addr()
	// 	found, dirs, missing, err := s.fetcher.blocksCache.Get(context.Background(), key)
	// 	require.NoError(t, err)
	// 	require.Equal(t, 1, len(found))
	// 	require.Equal(t, 0, len(missing))
	// 	require.Equal(t, int32(0), dirs[0].refCount.Load())
	// }
}

func createMatchingBlockRef(checksum uint32) BlockRef {
	block := createBlockRef(0, math.MaxUint64, model.Time(0), model.Time(math.MaxInt64))
	block.Checksum = checksum
	return block
}

func createBlockRef(
	minFingerprint, maxFingerprint uint64,
	startTimestamp, endTimestamp model.Time,
) BlockRef {
	day := startTimestamp.Unix() / int64(24*time.Hour/time.Second)
	return BlockRef{
		Ref: Ref{
			TenantID:       "fake",
			TableName:      fmt.Sprintf("%d", day),
			Bounds:         v1.NewBounds(model.Fingerprint(minFingerprint), model.Fingerprint(maxFingerprint)),
			StartTimestamp: startTimestamp,
			EndTimestamp:   endTimestamp,
			Checksum:       0,
		},
	}
}
