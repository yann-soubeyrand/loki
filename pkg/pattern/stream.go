package pattern

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/pattern/drain"
	"github.com/grafana/loki/v3/pkg/pattern/iter"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

type stream struct {
	fp           model.Fingerprint
	labels       labels.Labels
	labelsString string
	labelHash    uint64
	patterns     *drain.Drain
	mtx          sync.Mutex

	lastTs int64
}

// Hard-coded ban on service names that are known to be too noisy.
// TODO: Replace this with a more reactive solution to automatically reduce pattern generation from noisy streams.
var bannedServiceNames = map[string]interface{}{
	"grafana-ruler/grafana-ruler": struct{}{},
}

func newStream(
	fp model.Fingerprint,
	labels labels.Labels,
	metrics *ingesterMetrics,
) (*stream, error) {
	return &stream{
		fp:           fp,
		labels:       labels,
		labelsString: labels.String(),
		labelHash:    labels.Hash(),
		patterns: drain.New(drain.DefaultConfig(), &drain.Metrics{
			PatternsEvictedTotal:  metrics.patternsDiscardedTotal,
			PatternsDetectedTotal: metrics.patternsDetectedTotal,
		}),
	}, nil
}

// shouldTrainPatterns evaluates an opinionated set of rules to whether to use this line to train patterns.
func (s *stream) shouldTrainPatterns(entry *logproto.Entry) bool {
	if len(entry.Line) < 10 {
		// Skip short lines
		return false
	}
	if entry.Line[0] == '{' && entry.Line[len(entry.Line)-1] == '}' {
		// Skip lines that look like JSON as they aren't intended to be human readable and don't give good patterns.
		return false
	}
	if _, ok := bannedServiceNames[s.labels.Get("service_name")]; ok {
		// Skip lines from services that we know are too noisy.
		return false
	}
	return true
}

func (s *stream) Push(
	_ context.Context,
	entries []logproto.Entry,
) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, entry := range entries {
		if entry.Timestamp.UnixNano() < s.lastTs {
			continue
		}
		s.lastTs = entry.Timestamp.UnixNano()
		if !s.shouldTrainPatterns(&entry) {
			continue
		}
		s.patterns.Train(entry.Line, entry.Timestamp.UnixNano())
	}
	return nil
}

func (s *stream) Iterator(_ context.Context, from, through, step model.Time) (iter.Iterator, error) {
	// todo we should improve locking.
	s.mtx.Lock()
	defer s.mtx.Unlock()

	clusters := s.patterns.Clusters()
	iters := make([]iter.Iterator, 0, len(clusters))

	for _, cluster := range clusters {
		if cluster.String() == "" {
			continue
		}
		iters = append(iters, cluster.Iterator(from, through, step))
	}
	return iter.NewMerge(iters...), nil
}

func (s *stream) prune(olderThan time.Duration) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	clusters := s.patterns.Clusters()
	for _, cluster := range clusters {
		cluster.Prune(olderThan)
		if cluster.Size == 0 {
			s.patterns.Delete(cluster)
		}
	}

	return len(s.patterns.Clusters()) == 0
}
