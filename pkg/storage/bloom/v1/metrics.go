package v1

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/loki/v3/pkg/util/constants"
)

type Metrics struct {
	sbfCreationTime     prometheus.Counter   // time spent creating sbfs
	bloomSize           prometheus.Histogram // size of the bloom filter in bytes
	hammingWeightRatio  prometheus.Histogram // ratio of the hamming weight of the bloom filter to the number of bits in the bloom filter
	estimatedCount      prometheus.Histogram // estimated number of elements in the bloom filter
	chunksIndexed       *prometheus.CounterVec
	chunksPerSeries     prometheus.Histogram
	blockSeriesIterated prometheus.Counter
	tokensTotal         prometheus.Counter
	insertsTotal        *prometheus.CounterVec

	pagesRead    *prometheus.CounterVec
	pagesSkipped *prometheus.CounterVec
	bytesRead    *prometheus.CounterVec
	bytesSkipped *prometheus.CounterVec

	pageLen                 *prometheus.HistogramVec
	pageSeries              *prometheus.HistogramVec
	bloomChunks             *prometheus.HistogramVec
	bloomTokens             *prometheus.HistogramVec
	bloomSourceBytes        *prometheus.HistogramVec
	bloomLines              *prometheus.HistogramVec
	bloomLayers             *prometheus.HistogramVec
	bloomBytesSize          *prometheus.HistogramVec
	bloomLastLayerFillRatio *prometheus.HistogramVec
}

const (
	chunkIndexedTypeIterated = "iterated"
	chunkIndexedTypeCopied   = "copied"

	tokenTypeRaw           = "raw"
	tokenTypeChunkPrefixed = "chunk_prefixed"
	collisionTypeFalse     = "false"
	collisionTypeTrue      = "true"
	collisionTypeCache     = "cache"

	pageTypeBloom  = "bloom"
	pageTypeSeries = "series"

	skipReasonTooLarge = "too_large"
	skipReasonErr      = "err"
	skipReasonOOB      = "out_of_bounds"
)

func NewMetrics(r prometheus.Registerer) *Metrics {
	return &Metrics{
		sbfCreationTime: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_creation_time_total",
			Help:      "Time spent creating scalable bloom filters",
		}),
		bloomSize: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_size",
			Help:      "Size of the bloom filter in bytes",
			Buckets:   prometheus.ExponentialBucketsRange(1<<10, 512<<20, 10),
		}),
		hammingWeightRatio: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_hamming_weight_ratio",
			Help:      "Ratio of the hamming weight of the bloom filter to the number of bits in the bloom filter",
			Buckets:   prometheus.ExponentialBucketsRange(0.001, 1, 12),
		}),
		estimatedCount: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_estimated_count",
			Help:      "Estimated number of elements in the bloom filter",
			Buckets:   prometheus.ExponentialBucketsRange(1, 33554432, 10),
		}),
		chunksIndexed: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_chunks_indexed_total",
			Help:      "Number of chunks indexed in bloom filters, partitioned by type. Type can be iterated or copied, where iterated indicates the chunk data was fetched and ngrams for it's contents generated whereas copied indicates the chunk already existed in another source block and was copied to the new block",
		}, []string{"type"}),
		chunksPerSeries: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_chunks_per_series",
			Help:      "Number of chunks per series. Can be copied from an existing bloom or iterated",
			Buckets:   prometheus.ExponentialBucketsRange(1, 4096, 10),
		}),
		blockSeriesIterated: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_block_series_iterated_total",
			Help:      "Number of series iterated in existing blocks while generating new blocks",
		}),
		tokensTotal: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_tokens_total",
			Help:      "Number of tokens processed",
		}),
		insertsTotal: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_inserts_total",
			Help:      "Number of inserts into the bloom filter. collision type may be `false` (no collision), `cache` (found in token cache) or true (found in bloom filter). token_type may be either `raw` (the original ngram) or `chunk_prefixed` (the ngram with the chunk prefix)",
		}, []string{"token_type", "collision"}),

		pagesRead: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_pages_read_total",
			Help:      "Number of bloom pages read",
		}, []string{"type"}),
		pagesSkipped: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_pages_skipped_total",
			Help:      "Number of bloom pages skipped during query iteration",
		}, []string{"type", "reason"}),
		bytesRead: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_bytes_read_total",
			Help:      "Number of bytes read from bloom pages",
		}, []string{"type"}),
		bytesSkipped: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Namespace: constants.Loki,
			Name:      "bloom_bytes_skipped_total",
			Help:      "Number of bytes skipped during query iteration",
		}, []string{"type", "reason"}),

		pageLen: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_page_len",
			Help:      "Length of a bloom page",
			Buckets:   prometheus.ExponentialBucketsRange(1<<10, 512<<20, 10),
		}, []string{"size"}),
		pageSeries: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_page_series",
			Help:      "Number of series in a page",
			Buckets:   prometheus.ExponentialBucketsRange(1, 4096, 10),
		}, []string{"size"}),
		bloomChunks: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_chunks",
			Help:      "Number of chunks in a bloom",
			Buckets:   prometheus.ExponentialBucketsRange(1, 4096, 10),
		}, []string{"size"}),
		bloomTokens: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_tokens",
			Help:      "Number of tokens in a bloom",
			Buckets:   prometheus.ExponentialBucketsRange(1, 33554432, 10),
		}, []string{"size"}),
		bloomSourceBytes: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_source_bytes",
			Help:      "Number of bytes in a bloom source",
			Buckets:   prometheus.ExponentialBucketsRange(1<<10, 512<<20, 10),
		}, []string{"size"}),
		bloomLines: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_lines",
			Help:      "Number of lines in a bloom",
			Buckets:   prometheus.ExponentialBucketsRange(1, 33554432, 10),
		}, []string{"size"}),
		bloomLayers: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_layers",
			Help:      "Number of layers in a bloom",
			Buckets:   prometheus.LinearBuckets(1, 1, 12),
		}, []string{"size"}),
		bloomBytesSize: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_bytes_size",
			Help:      "Number of bytes in a bloom",
			Buckets:   prometheus.ExponentialBucketsRange(1<<10, 512<<20, 10),
		}, []string{"size"}),
		bloomLastLayerFillRatio: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "bloom_exp_last_layer_fill_ratio",
			Help:      "Number of bytes in a bloom",
			Buckets:   prometheus.LinearBuckets(0, 0.1, 10),
		}, []string{"size"}),
	}
}

func (m *Metrics) RecordPageMetrics(page BloomPageHeader) {
	size := "small"
	if page.Len > 64<<20 { // 64MB
		size = "large"
	}
	m.pageSeries.WithLabelValues(size).Observe(float64(page.Stats.Series))
	m.pageLen.WithLabelValues(size).Observe(float64(page.Len))
}

func (m *Metrics) RecordBloomMetrics(bloom *Bloom) {
	bytesSize := bloom.BytesSize()
	size := "small"
	if bytesSize > 64<<20 { // 64MB
		size = "large"
	}

	m.bloomChunks.WithLabelValues(size).Observe(float64(bloom.Stats.Chunks))
	m.bloomTokens.WithLabelValues(size).Observe(float64(bloom.Stats.Tokens))
	m.bloomSourceBytes.WithLabelValues(size).Observe(float64(bloom.Stats.Bytes))
	m.bloomLines.WithLabelValues(size).Observe(float64(bloom.Stats.Lines))
	m.bloomBytesSize.WithLabelValues(size).Observe(float64(bloom.BytesSize()))

	layers := bloom.FillRatioPerLayer()
	fillRatioLastLayer := layers[len(layers)-1]

	m.bloomLayers.WithLabelValues(size).Observe(float64(len(layers)))
	m.bloomLastLayerFillRatio.WithLabelValues(size).Observe(fillRatioLastLayer)
}
