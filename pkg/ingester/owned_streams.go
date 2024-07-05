package ingester

import (
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"go.uber.org/atomic"

	"github.com/grafana/loki/v3/pkg/util/constants"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
)

var notOwnedStreamsMetric = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: constants.Loki,
	Name:      "ingester_not_owned_streams",
	Help:      "The total number of not owned streams in memory.",
})

var nowOwnedStreamsCount = atomic.NewUint64(0)

type ownedStreamService struct {
	tenantID         string
	limiter          *Limiter
	fixedLimit       *atomic.Int32
	ownedStreamCount int
	lock             sync.RWMutex
	notOwnedStreams  map[model.Fingerprint]any
}

func newOwnedStreamService(tenantID string, limiter *Limiter) *ownedStreamService {
	svc := &ownedStreamService{
		tenantID:        tenantID,
		limiter:         limiter,
		fixedLimit:      atomic.NewInt32(0),
		notOwnedStreams: make(map[model.Fingerprint]any),
	}

	svc.updateFixedLimit()
	return svc
}

func (s *ownedStreamService) getOwnedStreamCount() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.ownedStreamCount
}

func (s *ownedStreamService) updateFixedLimit() (old, new int32) {
	newLimit, _, _, _ := s.limiter.GetStreamCountLimit(s.tenantID)
	return s.fixedLimit.Swap(int32(newLimit)), int32(newLimit)

}

func (s *ownedStreamService) getFixedLimit() int {
	return int(s.fixedLimit.Load())
}

func (s *ownedStreamService) trackStreamOwnership(fp model.Fingerprint, owned bool, labelsString string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if owned {
		if _, markedNotOwned := s.notOwnedStreams[fp]; markedNotOwned {
			level.Warn(log.With(util_log.Logger, "svc", "ownedStreamService")).Log("msg", "stream reported as owned but it's still marked as not owned", "fp", fp, "labels", labelsString)
		}
		s.ownedStreamCount++
		return
	}
	nowOwnedStreamsCount.Inc()
	notOwnedStreamsMetric.Inc()
	s.notOwnedStreams[fp] = nil
}

func (s *ownedStreamService) trackRemovedStream(fp model.Fingerprint) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, notOwned := s.notOwnedStreams[fp]; notOwned {
		nowOwnedStreamsCount.Dec()
		notOwnedStreamsMetric.Dec()
		delete(s.notOwnedStreams, fp)
		return
	}
	s.ownedStreamCount--
}

func (s *ownedStreamService) resetStreamCounts() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ownedStreamCount = 0
	nowOwnedStreamsCount.Sub(uint64(len(s.notOwnedStreams)))
	notOwnedStreamsMetric.Sub(float64(len(s.notOwnedStreams)))
	s.notOwnedStreams = make(map[model.Fingerprint]any)
}

func (s *ownedStreamService) isStreamNotOwned(fp model.Fingerprint, labelsString string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, notOwned := s.notOwnedStreams[fp]
	level.Info(log.With(util_log.Logger, "svc", "ownedStreamService")).Log("msg", "checking ownership for a stream", "fp", fp, "labels", labelsString, "notOwned", notOwned)
	return notOwned
}
