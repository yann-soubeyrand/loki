package pattern

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/distributor"
	"github.com/grafana/loki/v3/pkg/ingester"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	lokiring "github.com/grafana/loki/v3/pkg/util/ring"
)

type Tee struct {
	cfg        Config
	logger     log.Logger
	ringClient *RingClient

	ingesterAppends *prometheus.CounterVec
}

func NewTee(
	cfg Config,
	ringClient *RingClient,
	metricsNamespace string,
	registerer prometheus.Registerer,
	logger log.Logger,
) (*Tee, error) {
	registerer = prometheus.WrapRegistererWithPrefix(metricsNamespace+"_", registerer)

	t := &Tee{
		logger: log.With(logger, "component", "pattern-tee"),
		ingesterAppends: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Name: "pattern_ingester_appends_total",
			Help: "The total number of batch appends sent to pattern ingesters.",
		}, []string{"ingester", "status"}),
		cfg:        cfg,
		ringClient: ringClient,
	}

	return t, nil
}

func stringWithoutLabel(ls labels.Labels, label string) string {
	var b bytes.Buffer

	b.WriteByte('{')
	i := 0
	ls.Range(func(l labels.Label) {
		if l.Name == label {
			return
		}
		if i > 0 {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
		i++
	})
	b.WriteByte('}')
	return b.String()
}

// Duplicate Implements distributor.Tee which is used to tee distributor requests to pattern ingesters.
func (t *Tee) Duplicate(tenant string, streams []distributor.KeyedStream) {
	for idx := range streams {
		go func(stream distributor.KeyedStream) {
			if err := t.sendStream(tenant, stream); err != nil {
				level.Error(t.logger).Log("msg", "failed to send stream to pattern ingester", "err", err)
			}
		}(streams[idx])
	}
}

func (t *Tee) sendStream(tenant string, stream distributor.KeyedStream) error {
	if strings.Contains(stream.Stream.Labels, ingester.ShardLbName) {
		lbls, _ := syntax.ParseLabels(stream.Stream.Labels)
		newLabels := stringWithoutLabel(lbls, ingester.ShardLbName)
		stream.HashKey = lokiring.TokenFor(tenant, newLabels)
		stream.Stream.Labels = newLabels
		stream.Stream.Hash, _ = lbls.HashWithoutLabels(make([]byte, 0), ingester.ShardLbName)
	}
	var descs [1]ring.InstanceDesc
	replicationSet, err := t.ringClient.ring.Get(stream.HashKey, ring.WriteNoExtend, descs[:0], nil, nil)
	if err != nil {
		return err
	}
	if replicationSet.Instances == nil {
		return errors.New("no instances found")
	}
	addr := replicationSet.Instances[0].Addr
	client, err := t.ringClient.pool.GetClientFor(addr)
	if err != nil {
		return err
	}
	req := &logproto.PushRequest{
		Streams: []logproto.Stream{
			stream.Stream,
		},
	}

	ctx, cancel := context.WithTimeout(user.InjectOrgID(context.Background(), tenant), t.cfg.ClientConfig.RemoteTimeout)
	defer cancel()
	_, err = client.(logproto.PatternClient).Push(ctx, req)
	if err != nil {
		t.ingesterAppends.WithLabelValues(addr, "fail").Inc()
		return err
	}
	t.ingesterAppends.WithLabelValues(addr, "success").Inc()
	return nil
}
