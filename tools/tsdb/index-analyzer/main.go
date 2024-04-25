package main

import (
	"flag"
	"fmt"
	"golang.org/x/exp/slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/loki/v3/pkg/storage"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/tools/tsdb/helpers"
)

// go build ./tools/tsdb/index-analyzer && BUCKET=19453 DIR=/tmp/loki-index-analysis ./index-analyzer --config.file=/tmp/loki-config.yaml
func main() {
	conf, _, bucket, err := helpers.Setup()
	helpers.ExitErr("setting up", err)

	_, overrides, clientMetrics := helpers.DefaultConfigs()

	flag.Parse()

	buckets := strings.Split(bucket, ",")
	var allAnalysis []stats
	for i, bucket := range buckets {
		periodCfg, tableRange, tableName, err := helpers.GetPeriodConfigForTableNumber(bucket, conf.SchemaConfig.Configs)
		helpers.ExitErr("find period config for bucket", err)

		objectClient, err := storage.NewObjectClient(periodCfg.ObjectType, conf.StorageConfig, clientMetrics)
		helpers.ExitErr("creating object client", err)

		shipper, err := indexshipper.NewIndexShipper(
			periodCfg.IndexTables.PathPrefix,
			conf.StorageConfig.TSDBShipperConfig,
			objectClient,
			overrides,
			nil,
			tsdb.OpenShippableTSDB,
			tableRange,
			prometheus.WrapRegistererWithPrefix(string(byte(97+i)), prometheus.DefaultRegisterer),
			util_log.Logger,
		)
		helpers.ExitErr("creating index shipper", err)

		tenants, err := helpers.ResolveTenants(objectClient, periodCfg.IndexTables.PathPrefix, tableName)
		helpers.ExitErr("resolving tenants", err)

		err, analysis := analyze(shipper, tableName, tenants)
		helpers.ExitErr("analyzing", err)

		allAnalysis = append(allAnalysis, analysis...)
	}

	// sort analysis by the series and the table
	slices.SortFunc(allAnalysis, func(a, b stats) int {
		if a.series < b.series {
			return -1
		}
		if a.series > b.series {
			return 1
		}
		return strings.Compare(a.table, b.table)
	})

	fmt.Printf("\n\n\n------------------STATS----------------------------\n")
	fmt.Printf("series, table, found, chunks, bytes, lines\n")
	for _, stats := range allAnalysis {
		fmt.Printf("%s, %s, %v, %d, %d, %d\n", stats.series, stats.table, stats.found, stats.chunks, stats.B, stats.lines)
	}

	fmt.Printf("\n\n\n------------------LABELS------------------------------\n")
	for _, stats := range allAnalysis {
		fmt.Printf("`%s`\n", stats.labels)
	}

}
