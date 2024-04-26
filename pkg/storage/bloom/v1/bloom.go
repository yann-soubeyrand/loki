package v1

import (
	"bytes"
	"fmt"
	"github.com/prometheus/common/model"
	"io"

	"github.com/go-kit/log/level"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/pkg/errors"

	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/grafana/loki/v3/pkg/storage/bloom/v1/filter"
	"github.com/grafana/loki/v3/pkg/util/encoding"
)

// NB(chaudum): Some block pages are way bigger than others (400MiB and
// bigger), and loading multiple pages into memory in parallel can cause the
// gateways to OOM.
// Figure out a decent default maximum page size that we can process.
var DefaultMaxPageSize = 64 << 20 // 64MB
var ErrPageTooLarge = errors.Errorf("bloom page too large")

type BloomStats struct {
	Chunks, Lines, Bytes, Tokens uint64
}

func (b *BloomStats) Add(other BloomStats) {
	b.Chunks += other.Chunks
	b.Lines += other.Lines
	b.Bytes += other.Bytes
	b.Tokens += other.Tokens
}

func (b *BloomStats) Reset() {
	b.Chunks = 0
	b.Lines = 0
	b.Bytes = 0
	b.Tokens = 0
}

func (s *BloomStats) Encode(enc *encoding.Encbuf) {
	enc.PutBE64(s.Chunks)
	enc.PutBE64(s.Lines)
	enc.PutBE64(s.Bytes)
	enc.PutBE64(s.Tokens)
}

func (s *BloomStats) Decode(dec *encoding.Decbuf) error {
	s.Chunks = dec.Be64()
	s.Lines = dec.Be64()
	s.Bytes = dec.Be64()
	s.Tokens = dec.Be64()
	return dec.Err()
}

type Bloom struct {
	filter.ScalableBloomFilter
	Stats BloomStats
}

func (b *Bloom) Encode(enc *encoding.Encbuf, version byte) error {
	// divide by 8 b/c bloom capacity is measured in bits, but we want bytes
	buf := bytes.NewBuffer(BlockPool.Get(int(b.Capacity() / 8)))

	// TODO(owen-d): have encoder implement writer directly so we don't need
	// to indirect via a buffer
	_, err := b.WriteTo(buf)
	if err != nil {
		return errors.Wrap(err, "encoding bloom filter")
	}

	data := buf.Bytes()
	enc.PutUvarint(len(data)) // length of bloom filter
	enc.PutBytes(data)
	BlockPool.Put(data[:0]) // release to pool

	if version >= V2 {
		b.Stats.Encode(enc)
	}

	return nil
}

func (b *Bloom) DecodeCopy(dec *encoding.Decbuf, version byte) error {
	ln := dec.Uvarint()
	data := dec.Bytes(ln)

	if version >= V2 {
		if err := b.Stats.Decode(dec); err != nil {
			return errors.Wrap(err, "decoding bloom stats")
		}
	}

	_, err := b.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "decoding copy of bloom filter")
	}

	return nil
}

func (b *Bloom) Decode(dec *encoding.Decbuf, version byte) error {
	ln := dec.Uvarint()
	data := dec.Bytes(ln)

	if version >= V2 {
		if err := b.Stats.Decode(dec); err != nil {
			return errors.Wrap(err, "decoding bloom stats")
		}
	}

	_, err := b.DecodeFrom(data)
	if err != nil {
		return errors.Wrap(err, "decoding bloom filter")
	}

	return nil
}

func LazyDecodeBloomPage(r io.Reader, pool chunkenc.ReaderPool, page BloomPageHeader, version byte) (*BloomPageDecoder, error) {
	data := BlockPool.Get(page.Len)[:page.Len]
	defer BlockPool.Put(data)

	_, err := io.ReadFull(r, data)
	if err != nil {
		return nil, errors.Wrap(err, "reading bloom page")
	}
	dec := encoding.DecWith(data)

	if err := dec.CheckCrc(castagnoliTable); err != nil {
		return nil, errors.Wrap(err, "checksumming bloom page")
	}

	decompressor, err := pool.GetReader(bytes.NewReader(dec.Get()))
	if err != nil {
		return nil, errors.Wrap(err, "getting decompressor")
	}
	defer pool.PutReader(decompressor)

	b := BlockPool.Get(page.DecompressedLen)[:page.DecompressedLen]

	if _, err = io.ReadFull(decompressor, b); err != nil {
		return nil, errors.Wrap(err, "decompressing bloom page")
	}

	decoder := NewBloomPageDecoder(b, version)

	return decoder, nil
}

// shortcut to skip allocations when we know the page is not compressed
func LazyDecodeBloomPageNoCompression(r io.Reader, page BloomPageHeader, version byte) (*BloomPageDecoder, error) {
	// data + checksum
	if page.Len != page.DecompressedLen+4 {
		return nil, errors.New("the Len and DecompressedLen of the page do not match")
	}
	data := BlockPool.Get(page.Len)[:page.Len]

	_, err := io.ReadFull(r, data)
	if err != nil {
		return nil, errors.Wrap(err, "reading bloom page")
	}
	dec := encoding.DecWith(data)

	if err := dec.CheckCrc(castagnoliTable); err != nil {
		return nil, errors.Wrap(err, "checksumming bloom page")
	}

	return NewBloomPageDecoder(dec.Get(), version), nil
}

func NewBloomPageDecoder(data []byte, version byte) *BloomPageDecoder {
	// last 8 bytes are the number of blooms in this page
	dec := encoding.DecWith(data[len(data)-8:])
	n := int(dec.Be64())
	// reset data to the bloom portion of the page
	data = data[:len(data)-8]
	dec.B = data

	// reset data to the bloom portion of the page

	decoder := &BloomPageDecoder{
		version: version,
		dec:     &dec,
		data:    data,
		n:       n,
	}

	return decoder
}

// Decoder is a seekable, reset-able iterator
// TODO(owen-d): use buffer pools. The reason we don't currently
// do this is because the `data` slice currently escapes the decoder
// via the returned bloom, so we can't know when it's safe to return it to the pool.
// This happens via `data ([]byte) -> dec (*encoding.Decbuf) -> bloom (Bloom)` where
// the final Bloom has a reference to the data slice.
// We could optimize this by encoding the mode (read, write) into our structs
// and doing copy-on-write shenannigans, but I'm avoiding this for now.
type BloomPageDecoder struct {
	version byte
	data    []byte
	dec     *encoding.Decbuf

	n   int // number of blooms in page
	cur *Bloom
	err error
}

// Relinquish returns the underlying byte slice to the pool
// for efficiency. It's intended to be used as a
// perf optimization.
// This can only safely be used when the underlying bloom
// bytes don't escape the decoder:
// on reads in the bloom-gw but not in the bloom-compactor
func (d *BloomPageDecoder) Relinquish() {
	data := d.data
	d.data = nil

	if cap(data) > 0 {
		BlockPool.Put(data)
	}
}

func (d *BloomPageDecoder) Reset() {
	d.err = nil
	d.cur = nil
	d.dec.B = d.data
}

func (d *BloomPageDecoder) Seek(offset int) {
	d.dec.B = d.data[offset:]
}

func (d *BloomPageDecoder) Next() bool {
	// end of iteration, no error
	if d.dec.Len() == 0 {
		return false
	}

	var b Bloom
	d.err = b.Decode(d.dec, d.version)
	// end of iteration, error
	if d.err != nil {
		return false
	}
	d.cur = &b
	return true
}

func (d *BloomPageDecoder) At() *Bloom {
	return d.cur
}

func (d *BloomPageDecoder) Err() error {
	return d.err
}

type BloomPageStats struct {
	BloomStats
	Series, Blooms uint64
}

func (s *BloomPageStats) Add(other *BloomPageStats) {
	if other == nil {
		return
	}

	s.Series += other.Series
	s.Blooms += other.Blooms
	s.BloomStats.Add(other.BloomStats)
}

func (s *BloomPageStats) Reset() {
	s.Series = 0
	s.Blooms = 0
	s.BloomStats.Reset()
}

func (s *BloomPageStats) Encode(enc *encoding.Encbuf) {
	enc.PutBE64(s.Series)
	enc.PutBE64(s.Blooms)
	s.BloomStats.Encode(enc)
}

func (s *BloomPageStats) Decode(dec *encoding.Decbuf) error {
	s.Series = dec.Be64()
	s.Blooms = dec.Be64()
	if err := s.BloomStats.Decode(dec); err != nil {
		return errors.Wrap(err, "decoding bloom page stats")
	}
	return dec.Err()
}

type BloomPageHeader struct {
	N, Offset, Len, DecompressedLen int
	Stats                           BloomPageStats
}

func (h *BloomPageHeader) Encode(enc *encoding.Encbuf, version byte) {
	enc.PutUvarint(h.N)
	enc.PutUvarint(h.Offset)
	enc.PutUvarint(h.Len)
	enc.PutUvarint(h.DecompressedLen)

	if version >= V2 {
		h.Stats.Encode(enc)
	}
}

func (h *BloomPageHeader) Decode(dec *encoding.Decbuf, version byte) error {
	h.N = dec.Uvarint()
	h.Offset = dec.Uvarint()
	h.Len = dec.Uvarint()
	h.DecompressedLen = dec.Uvarint()

	if version >= V2 {
		if err := h.Stats.Decode(dec); err != nil {
			return errors.Wrap(err, "decoding bloom page stats")
		}
	}
	return dec.Err()
}

type BloomBlock struct {
	schema      Schema
	pageHeaders []BloomPageHeader
}

func NewBloomBlock(encoding chunkenc.Encoding) BloomBlock {
	return BloomBlock{
		schema: Schema{version: DefaultSchemaVersion, encoding: encoding},
	}
}

func (b *BloomBlock) DecodeHeaders(r io.ReadSeeker) (uint32, error) {
	if err := b.schema.DecodeFrom(r); err != nil {
		return 0, errors.Wrap(err, "decoding schema")
	}

	var (
		err error
		dec encoding.Decbuf
	)
	// last 12 bytes are (headers offset: 8 byte u64, checksum: 4 byte u32)
	if _, err := r.Seek(-12, io.SeekEnd); err != nil {
		return 0, errors.Wrap(err, "seeking to bloom headers metadata")
	}
	dec.B, err = io.ReadAll(r)
	if err != nil {
		return 0, errors.Wrap(err, "reading bloom headers metadata")
	}

	headerOffset := dec.Be64()
	checksum := dec.Be32()

	if _, err := r.Seek(int64(headerOffset), io.SeekStart); err != nil {
		return 0, errors.Wrap(err, "seeking to bloom headers")
	}
	dec.B, err = io.ReadAll(r)
	if err != nil {
		return 0, errors.Wrap(err, "reading bloom page headers")
	}

	if err := dec.CheckCrc(castagnoliTable); err != nil {
		return 0, errors.Wrap(err, "checksumming page headers")
	}

	b.pageHeaders = make([]BloomPageHeader, dec.Uvarint())
	for i := 0; i < len(b.pageHeaders); i++ {
		header := &b.pageHeaders[i]
		if err := header.Decode(&dec, b.schema.version); err != nil {
			return 0, errors.Wrapf(err, "decoding %dth series header", i)
		}
	}
	return checksum, nil
}

func (b *BloomBlock) BloomPageDecoder(r io.ReadSeeker, pageIdx int, maxPageSize int, metrics *Metrics, s ...model.Fingerprint) (res *BloomPageDecoder, err error) {
	if pageIdx < 0 || pageIdx >= len(b.pageHeaders) {
		metrics.pagesSkipped.WithLabelValues(pageTypeBloom, skipReasonOOB).Inc()
		metrics.bytesSkipped.WithLabelValues(pageTypeBloom, skipReasonOOB).Add(float64(b.pageHeaders[pageIdx].DecompressedLen))
		return nil, fmt.Errorf("invalid page (%d) for bloom page decoding", pageIdx)
	}

	var series model.Fingerprint
	if len(s) > 0 {
		series = s[0]
	}

	page := b.pageHeaders[pageIdx]
	// fmt.Printf("pageIdx=%d page=%+v size=%.2fMiB\n", pageIdx, page, float64(page.Len)/float64(1<<20))

	metrics.RecordPageMetrics(page)

	if page.Len > maxPageSize {
		metrics.pagesSkipped.WithLabelValues(pageTypeBloom, skipReasonTooLarge).Inc()
		metrics.bytesSkipped.WithLabelValues(pageTypeBloom, skipReasonTooLarge).Add(float64(page.DecompressedLen))

		if _, err = r.Seek(int64(page.Offset), io.SeekStart); err != nil {
			panic(err)
		}

		if b.schema.encoding == chunkenc.EncNone {
			res, err = LazyDecodeBloomPageNoCompression(r, page, b.schema.version)
		} else {
			res, err = LazyDecodeBloomPage(r, b.schema.DecompressorPool(), page, b.schema.version)
		}
		if err != nil {
			panic(err)
		}
		res.Next()
		bloom := res.At()

		metrics.RecordBloomMetrics(bloom)

		var capacityPerLayer string
		for i, layer := range bloom.CapacityPerLayer() {
			if i > 0 {
				capacityPerLayer += ", "
			}
			capacityPerLayer += fmt.Sprintf("%d", layer)
		}

		var fillRatioLastLayer float64
		var fillRatioPerLayer string
		for i, layer := range bloom.FillRatioPerLayer() {
			if i > 0 {
				fillRatioPerLayer += ", "
			}
			fillRatioPerLayer += fmt.Sprintf("%.2f", layer)
			fillRatioLastLayer = layer
		}

		var countPerLayer string
		for i, layer := range bloom.CountPerLayer() {
			if i > 0 {
				countPerLayer += ", "
			}
			countPerLayer += fmt.Sprintf("%d", layer)
		}

		var bytesPerLayer string
		for i, layer := range bloom.BytesSizePerLayer() {
			if i > 0 {
				bytesPerLayer += ", "
			}

			var layerSum uint
			var layerSizes string
			for j, size := range layer {
				if j > 0 {
					layerSizes += ", "
				}
				layerSizes += fmt.Sprintf("%d", size)
				layerSum += size
			}

			bytesPerLayer += fmt.Sprintf("{%d: [%s]}", layerSum, layerSizes)
		}

		var fpRatesPerLayer string
		for i, layer := range bloom.FPRatePerLayer() {
			if i > 0 {
				fpRatesPerLayer += ", "
			}
			fpRatesPerLayer += fmt.Sprintf("%f", layer)
		}

		level.Error(util_log.Logger).Log(
			"msg", "page too large",
			"series_fp", series.String(),
			"version", b.schema.version,
			"page", pageIdx,
			"len", page.Len,
			"decompressedLen", page.DecompressedLen,
			"max", maxPageSize,
			"blooms", page.N,
			"stats_blooms", page.Stats.Blooms,
			"series", page.Stats.Series,
			"chunks", page.Stats.Chunks,
			"lines", page.Stats.Lines,
			"bytes", page.Stats.Bytes,
			"tokens", page.Stats.Tokens,
			"capacity", bloom.Capacity(),
			"capacityPerLayer", capacityPerLayer,
			"fillRatio", bloom.FillRatio(),
			"fillRatioPerLayer", fillRatioPerLayer,
			"fillRatioLastLayer", fillRatioLastLayer,
			"countPerLayer", countPerLayer,
			"bloomBytes", bloom.BytesSize(),
			"bytesPerLayer", bytesPerLayer,
			"fpRatesPerLayer", fpRatesPerLayer,
		)

		return nil, fmt.Errorf("bloom too big for series %s - bytes(%d) origBytes(%d) layers(%d) fillRatio(%.2f) lastLayerFillRatio(%.2f) capacity(%d) capacityPerLayer(%s) countPerLayer(%s) bytesPerLayer(%s) fpRatesPerLayer(%s)",
			series, bloom.BytesSize(), page.Stats.Bytes, len(bloom.CapacityPerLayer()), bloom.FillRatio(), fillRatioLastLayer, bloom.Capacity(), capacityPerLayer, countPerLayer, bytesPerLayer, fpRatesPerLayer)
	}

	if _, err = r.Seek(int64(page.Offset), io.SeekStart); err != nil {
		metrics.pagesSkipped.WithLabelValues(pageTypeBloom, skipReasonErr).Inc()
		metrics.bytesSkipped.WithLabelValues(pageTypeBloom, skipReasonErr).Add(float64(page.DecompressedLen))
		return nil, errors.Wrap(err, "seeking to bloom page")
	}

	if b.schema.encoding == chunkenc.EncNone {
		res, err = LazyDecodeBloomPageNoCompression(r, page, b.schema.version)
	} else {
		res, err = LazyDecodeBloomPage(r, b.schema.DecompressorPool(), page, b.schema.version)
	}

	if err != nil {
		metrics.pagesSkipped.WithLabelValues(pageTypeBloom, skipReasonErr).Inc()
		metrics.bytesSkipped.WithLabelValues(pageTypeBloom, skipReasonErr).Add(float64(page.DecompressedLen))
		return nil, errors.Wrap(err, "decoding bloom page")
	}

	metrics.pagesRead.WithLabelValues(pageTypeBloom).Inc()
	metrics.bytesRead.WithLabelValues(pageTypeBloom).Add(float64(page.DecompressedLen))
	return res, nil
}
