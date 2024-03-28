package bloomshipper

import (
	"context"
	"fmt"
	"github.com/prometheus/common/model"
	"golang.org/x/exp/slices"
	"sort"

	v1 "github.com/grafana/loki/pkg/storage/bloom/v1"
)

type ForEachBlockCallback func(bq *v1.BlockQuerier, bounds v1.FingerprintBounds) error

type Interface interface {
	ForEach(ctx context.Context, tenant string, blocks []BlockRef, callback ForEachBlockCallback) error
	Stop()
}

type Shipper struct {
	store Store
}

type Limits interface {
	BloomGatewayBlocksDownloadingParallelism(tenantID string) int
}

func NewShipper(client Store) *Shipper {
	return &Shipper{store: client}
}

// ForEach is a convenience function that wraps the store's FetchBlocks function
// and automatically closes the block querier once the callback was run.
func (s *Shipper) ForEach(ctx context.Context, refs []BlockRef, callback ForEachBlockCallback) error {
	bqs, err := s.store.FetchBlocks(ctx, refs, WithFetchAsync(false))
	if err != nil {
		return err
	}

	if len(bqs) != len(refs) {
		return fmt.Errorf("number of response (%d) does not match number of requests (%d)", len(bqs), len(refs))
	}

	for i := range bqs {
		err := callback(bqs[i].BlockQuerier, bqs[i].Bounds)
		// close querier to decrement ref count
		bqs[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Shipper) Stop() {
	s.store.Stop()
}

// BlocksForMetas returns up to all block from the given metas for each fingerprints
func BlocksForMetas(metas []Meta, interval Interval, fingerprints []model.Fingerprint) []BlockRef {
	if len(fingerprints) == 0 {
		return nil
	}

	refs := make([]BlockRef, 0, len(fingerprints))
	for _, meta := range metas {
		for _, block := range meta.Blocks {
			// Filter out blocks that are outside the requested interval
			if !interval.Overlaps(block.Interval()) {
				continue
			}

			// Skip blocks that do not match any of the fingerprints
			if matchesFP := slices.ContainsFunc(fingerprints, func(fp model.Fingerprint) bool {
				return block.Bounds.Match(fp)
			}); !matchesFP {
				continue
			}

			refs = append(refs, block)
		}
	}

	sort.Slice(refs, func(i, j int) bool {
		return refs[i].Bounds.Less(refs[j].Bounds)
	})

	return refs
}
