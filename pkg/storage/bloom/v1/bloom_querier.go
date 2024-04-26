package v1

import (
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

type BloomQuerier interface {
	Seek(BloomOffset) (*Bloom, error)
}

type LazyBloomIter struct {
	usePool bool

	b *Block
	m int // max page size in bytes

	// state
	initialized  bool
	err          error
	curPageIndex int
	curPage      *BloomPageDecoder

	blockQuerierUuid string
	uuid             string
}

// NewLazyBloomIter returns a new lazy bloom iterator.
// If pool is true, the underlying byte slice of the bloom page
// will be returned to the pool for efficiency.
// This can only safely be used when the underlying bloom
// bytes don't escape the decoder.
func NewLazyBloomIter(b *Block, pool bool, maxSize int, blockQuerierUuid string) *LazyBloomIter {
	return &LazyBloomIter{
		usePool: pool,
		b:       b,
		m:       maxSize,

		blockQuerierUuid: blockQuerierUuid,
		uuid:             uuid.New().String(),
	}
}

func (it *LazyBloomIter) ensureInit() {
	// TODO(owen-d): better control over when to decode
	if !it.initialized {
		if err := it.b.LoadHeaders(); err != nil {
			it.err = err
		}
		it.initialized = true
	}
}

type LazyBloomIterInfo struct {
	series           *SeriesWithOffset
	seriesFP         model.Fingerprint
	uuidFusedQuerier string
}

func (it *LazyBloomIter) Seek(offset BloomOffset, infos ...LazyBloomIterInfo) {
	it.ensureInit()

	// if we need a different page or the current page hasn't been loaded,
	// load the desired page
	if it.curPageIndex != offset.Page || it.curPage == nil {

		// drop the current page if it exists and
		// we're using the pool
		if it.curPage != nil && it.usePool {
			it.curPage.Relinquish()
		}

		r, err := it.b.reader.Blooms()
		if err != nil {
			it.err = errors.Wrap(err, "getting blooms reader")
			return
		}
		decoder, err := it.b.blooms.BloomPageDecoder(r, offset.Page, it.m, it.b.metrics, BloomPageDecoderExtraInfo{
			from:             "lazy_bloom_iter_seek",
			series:           infos[0].series,
			seriesFP:         infos[0].seriesFP,
			uuidFusedQuerier: infos[0].uuidFusedQuerier,
			uuidBlockQuerier: it.blockQuerierUuid,
			uuidBloomIter:    it.uuid,
		})
		if err != nil {
			it.err = errors.Wrap(err, "loading bloom page")
			return
		}
		if it.err != nil {
			level.Error(util_log.Logger).Log(
				"msg", "iterator error is set but decoding succeeded",
			)
		}

		it.curPageIndex = offset.Page
		it.curPage = decoder
	}

	it.curPage.Seek(offset.ByteOffset)
}

func (it *LazyBloomIter) Next(infos ...LazyBloomIterInfo) bool {
	it.ensureInit()
	if it.err != nil {
		return false
	}
	return it.next(infos...)
}

func (it *LazyBloomIter) next(infos ...LazyBloomIterInfo) bool {
	if it.err != nil {
		return false
	}

	for it.curPageIndex < len(it.b.blooms.pageHeaders) {
		// first access of next page
		if it.curPage == nil {
			r, err := it.b.reader.Blooms()
			if err != nil {
				it.err = errors.Wrap(err, "getting blooms reader")
				return false
			}

			it.curPage, err = it.b.blooms.BloomPageDecoder(
				r,
				it.curPageIndex,
				it.m,
				it.b.metrics,
				BloomPageDecoderExtraInfo{
					from:             "lazy_bloom_iter_next",
					series:           infos[0].series,
					uuidFusedQuerier: infos[0].uuidFusedQuerier,
					uuidBlockQuerier: it.blockQuerierUuid,
					uuidBloomIter:    it.uuid,
				},
			)
			if err != nil {
				it.err = err
				return false
			}
			continue
		}

		if !it.curPage.Next() {
			// there was an error
			if it.curPage.Err() != nil {
				return false
			}

			// we've exhausted the current page, progress to next
			it.curPageIndex++
			// drop the current page if it exists and
			// we're using the pool
			if it.usePool {
				it.curPage.Relinquish()
			}
			it.curPage = nil
			continue
		}

		return true
	}

	// finished last page
	return false
}

func (it *LazyBloomIter) At() *Bloom {
	return it.curPage.At()
}

func (it *LazyBloomIter) Err() error {
	{
		if it.err != nil {
			return it.err
		}
		if it.curPage != nil {
			return it.curPage.Err()
		}
		return nil
	}
}
