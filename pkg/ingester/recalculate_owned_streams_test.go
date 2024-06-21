package ingester

import (
	"context"
	"fmt"
	"github.com/grafana/dskit/kv"
	"github.com/prometheus/client_golang/prometheus"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/validation"
)

func Benchmark_RecalculateOwnedStreams(b *testing.B) {
	tests := map[string]struct {
		streamsCountPerIngester int
		ingestersPerZone        int
	}{
		"100k_streams-99ingesters": {
			streamsCountPerIngester: 100_000,
			ingestersPerZone:        33,
		},
		"1kk_streams-300ingesters": {
			streamsCountPerIngester: 1_000_000,
			ingestersPerZone:        100,
		},
	}
	for name, tc := range tests {
		b.Run(name, func(b *testing.B) {
			//logger := log.NewLogfmtLogger(os.Stdout)
			logger := log.NewNopLogger()
			tenant := createTenant(b, tc.streamsCountPerIngester, logger, &mockCountRing{count: 3 * tc.ingestersPerZone})
			instancesSupplier := func() []*instance {
				return []*instance{tenant}
			}
			ingesterRing := createRing(b, logger, tc.ingestersPerZone)
			service := newRecalculateOwnedStreams(instancesSupplier, "ingester-zone-a-0", ingesterRing, time.Minute, logger)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				service.previousRing = ring.ReplicationSet{}
				service.recalculate()
			}
		})
	}
}

func createRing(b *testing.B, logger log.Logger, ingestersPerZone int) *ring.Ring {
	ringConfig := mockRingConfig(b, logger)

	ingesterRing, err := ring.New(ringConfig, "ingester", RingKey, logger, prometheus.NewRegistry())
	require.NoError(b, err)
	err = ingesterRing.KVClient.CAS(context.Background(), RingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := &ring.Desc{}
		tokensPerIngester := 512
		takenTokens := make(ring.Tokens, 0, 3*ingestersPerZone*tokensPerIngester)
		for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
			for i := 0; i < ingestersPerZone; i++ {
				ingesterID := fmt.Sprintf("ingester-%s-%d", zone, i)
				currentIngesterTokens := generateSortedTokens(tokensPerIngester, takenTokens)
				takenTokens = append(takenTokens, currentIngesterTokens...)
				desc.AddIngester(ingesterID, ingesterID, zone, currentIngesterTokens, ring.ACTIVE, time.Now())
			}
		}
		return desc, false, nil
	})
	err = ingesterRing.StartAsync(context.Background())
	require.NoError(b, err)
	err = ingesterRing.AwaitRunning(context.Background())
	require.NoError(b, err)
	return ingesterRing
}

func generateSortedTokens(numTokens int, allTakenTokens []uint32) ring.Tokens {
	gen := ring.NewRandomTokenGenerator()
	tokens := gen.GenerateTokens(numTokens, allTakenTokens)

	// Ensure generated tokens are sorted.
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})

	return tokens
}

func mockRingConfig(t testing.TB, logger log.Logger) ring.Config {
	kvClient, err := kv.NewClient(kv.Config{Store: "inmemory"}, ring.GetCodec(), nil, logger)
	require.NoError(t, err)

	config := ring.Config{}
	config.KVStore.Mock = kvClient
	config.HeartbeatTimeout = 9999 * time.Hour
	config.ReplicationFactor = 3
	config.ZoneAwarenessEnabled = true

	return config
}

type mockCountRing struct {
	count int
}

func (m *mockCountRing) HealthyInstancesCount() int {
	return m.count
}

func createTenant(b *testing.B, streamsCount int, logger log.Logger, ring RingCount) *instance {
	limits, err := validation.NewOverrides(validation.Limits{
		MaxGlobalStreamsPerUser: 100,
		UseOwnedStreamCount:     true,
	}, nil)
	require.NoError(b, err)
	limiter := NewLimiter(limits, NilMetrics, ring, 3)

	tenant := &instance{
		instanceID:      "fake",
		streams:         newStreamsMap(),
		ownedStreamsSvc: newOwnedStreamService("fake", limiter, logger),
		limiter:         limiter,
	}
	for i := 0; i < streamsCount; i++ {
		lbls := labels.FromStrings("app", fmt.Sprintf("application-%d", i))
		s := &stream{
			tenant:       "fake",
			labels:       lbls,
			labelsString: lbls.String(),
			fp:           model.Fingerprint(lbls.Hash()),
		}
		tenant.streams.StoreByFP(s.fp, s)
	}
	return tenant
}

//
//func Test_recalculateOwnedStreams_newRecalculateOwnedStreams(t *testing.T) {
//	mockInstancesSupplier := &mockTenantsSuplier{tenants: []*instance{}}
//	mockRing := newReadRingMock([]ring.InstanceDesc{
//		{Addr: "test", Timestamp: time.Now().UnixNano(), State: ring.ACTIVE, Tokens: []uint32{1, 2, 3}},
//	}, 0)
//	service := newRecalculateOwnedStreams(mockInstancesSupplier.get, "test", mockRing, 50*time.Millisecond, log.NewNopLogger())
//	require.Equal(t, 0, mockRing.getAllHealthyCallsCount, "ring must be called only after service's start up")
//	ctx := context.Background()
//	require.NoError(t, service.StartAsync(ctx))
//	require.NoError(t, service.AwaitRunning(ctx))
//	require.Eventually(t, func() bool {
//		return mockRing.getAllHealthyCallsCount >= 2
//	}, 1*time.Second, 50*time.Millisecond, "expected at least two runs of the iteration")
//}
//
//func Test_recalculateOwnedStreams_recalculate(t *testing.T) {
//	tests := map[string]struct {
//		featureEnabled              bool
//		expectedOwnedStreamCount    int
//		expectedNotOwnedStreamCount int
//	}{
//		"expected streams ownership to be recalculated": {
//			featureEnabled:              true,
//			expectedOwnedStreamCount:    4,
//			expectedNotOwnedStreamCount: 3,
//		},
//		"expected streams ownership recalculation to be skipped": {
//			featureEnabled:           false,
//			expectedOwnedStreamCount: 7,
//		},
//	}
//	for testName, testData := range tests {
//		t.Run(testName, func(t *testing.T) {
//			mockRing := &readRingMock{
//				replicationSet: ring.ReplicationSet{
//					Instances: []ring.InstanceDesc{{Addr: "ingester-0", Timestamp: time.Now().UnixNano(), State: ring.ACTIVE, Tokens: []uint32{100, 200, 300}}},
//				},
//				tokenRangesByIngester: map[string]ring.TokenRanges{
//					// this ingester owns token ranges [50, 100] and [200, 300]
//					"ingester-0": {50, 100, 200, 300},
//				},
//			}
//
//			limits, err := validation.NewOverrides(validation.Limits{
//				MaxGlobalStreamsPerUser: 100,
//				UseOwnedStreamCount:     testData.featureEnabled,
//			}, nil)
//			require.NoError(t, err)
//			limiter := NewLimiter(limits, NilMetrics, mockRing, 1)
//
//			tenant, err := newInstance(
//				defaultConfig(),
//				defaultPeriodConfigs,
//				"tenant-a",
//				limiter,
//				runtime.DefaultTenantConfigs(),
//				noopWAL{},
//				NilMetrics,
//				nil,
//				nil,
//				nil,
//				nil,
//				NewStreamRateCalculator(),
//				nil,
//				nil,
//			)
//			require.NoError(t, err)
//			require.Equal(t, 100, tenant.ownedStreamsSvc.getFixedLimit(), "MaxGlobalStreamsPerUser is 100 at this moment")
//			// not owned streams
//			createStream(t, tenant, 49)
//			createStream(t, tenant, 101)
//			createStream(t, tenant, 301)
//
//			// owned streams
//			createStream(t, tenant, 50)
//			createStream(t, tenant, 60)
//			createStream(t, tenant, 100)
//			createStream(t, tenant, 250)
//
//			require.Equal(t, 7, tenant.ownedStreamsSvc.ownedStreamCount)
//			require.Len(t, tenant.ownedStreamsSvc.notOwnedStreams, 0)
//
//			mockTenantsSupplier := &mockTenantsSuplier{tenants: []*instance{tenant}}
//
//			service := newRecalculateOwnedStreams(mockTenantsSupplier.get, "ingester-0", mockRing, 50*time.Millisecond, log.NewNopLogger())
//			//change the limit to assert that fixed limit is updated after the recalculation
//			limits.DefaultLimits().MaxGlobalStreamsPerUser = 50
//
//			service.recalculate()
//
//			if testData.featureEnabled {
//				require.Equal(t, 50, tenant.ownedStreamsSvc.getFixedLimit(), "fixed limit must be updated after recalculation")
//			}
//			require.Equal(t, testData.expectedOwnedStreamCount, tenant.ownedStreamsSvc.ownedStreamCount)
//			require.Len(t, tenant.ownedStreamsSvc.notOwnedStreams, testData.expectedNotOwnedStreamCount)
//		})
//	}
//
//}
//
//func Test_recalculateOwnedStreams_checkRingForChanges(t *testing.T) {
//	mockRing := &readRingMock{
//		replicationSet: ring.ReplicationSet{
//			Instances: []ring.InstanceDesc{{Addr: "ingester-0", Timestamp: time.Now().UnixNano(), State: ring.ACTIVE, Tokens: []uint32{100, 200, 300}}},
//		},
//	}
//	mockTenantsSupplier := &mockTenantsSuplier{tenants: []*instance{{}}}
//	service := newRecalculateOwnedStreams(mockTenantsSupplier.get, "ingester-0", mockRing, 50*time.Millisecond, log.NewNopLogger())
//
//	ringChanged, err := service.checkRingForChanges()
//	require.NoError(t, err)
//	require.True(t, ringChanged, "expected ring to be changed because it was not initialized yet")
//
//	ringChanged, err = service.checkRingForChanges()
//	require.NoError(t, err)
//	require.False(t, ringChanged, "expected ring not to be changed because token ranges is not changed")
//
//	anotherIngester := ring.InstanceDesc{Addr: "ingester-1", Timestamp: time.Now().UnixNano(), State: ring.ACTIVE, Tokens: []uint32{150, 250, 350}}
//	mockRing.replicationSet.Instances = append(mockRing.replicationSet.Instances, anotherIngester)
//
//	ringChanged, err = service.checkRingForChanges()
//	require.NoError(t, err)
//	require.True(t, ringChanged)
//}

func createStream(t *testing.T, inst *instance, fingerprint int) {
	lbls := labels.Labels{
		labels.Label{Name: "mock", Value: strconv.Itoa(fingerprint)}}

	_, _, err := inst.streams.LoadOrStoreNew(lbls.String(), func() (*stream, error) {
		return inst.createStreamByFP(lbls, model.Fingerprint(fingerprint))
	}, nil)
	require.NoError(t, err)
}

type mockTenantsSuplier struct {
	tenants []*instance
}

func (m *mockTenantsSuplier) get() []*instance {
	return m.tenants
}
