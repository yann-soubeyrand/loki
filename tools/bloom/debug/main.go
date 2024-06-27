package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/spanprofiler"
	"github.com/grafana/dskit/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"

	"github.com/grafana/loki/v3/pkg/bloombuild/builder"
	"github.com/grafana/loki/v3/pkg/bloombuild/protos"
	"github.com/grafana/loki/v3/pkg/loki"
	"github.com/grafana/loki/v3/pkg/runtime"
	v1 "github.com/grafana/loki/v3/pkg/storage/bloom/v1"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/bloomshipper"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/grafana/loki/v3/pkg/util/cfg"
	"github.com/grafana/loki/v3/pkg/validation"
)

func parseDayTime(s string) (dt config.DayTime, err error) {
	t, err := time.ParseInLocation("2006-01-02", s, time.UTC)
	if err != nil {
		return dt, err
	}
	return config.DayTime{
		Time: model.TimeFromUnix(t.Unix()),
	}, nil
}

func exitWithError(err string, args ...any) {
	fmt.Fprintf(os.Stderr, err, args...)
	os.Exit(1)
}

func main() {

	envFp := os.Getenv("FINGERPRINT") //  "6432507678569559861"
	if envFp == "" {
		exitWithError("env FINGERPRINT is required")
	}

	fp, err := strconv.ParseUint(envFp, 10, 64)
	if err != nil {
		exitWithError("failed to parse %s as uint64", envFp)
	}

	envTSDBIdent := os.Getenv("TSDB_IDENT") // "1719239625351731308-compactor-1718989696611-1719238677995-6b42663f.tsdb"
	if envTSDBIdent == "" {
		exitWithError("env TSBD_IDENT is required")
	}

	envDate := os.Getenv("DATE") // "2024-06-23"
	if envDate == "" {
		exitWithError("env DATE is required")
	}

	dayTime, err := parseDayTime(envDate)
	if err != nil {
		exitWithError("failed to parse %s as DayTime", envDate)
	}

	logger := log.NewLogfmtLogger(os.Stderr)
	logger = log.With(logger, "component", "bloom-builder")

	var LokiCfg loki.ConfigWrapper

	if loki.PrintVersion(os.Args[1:]) {
		fmt.Println(version.Print("loki"))
		os.Exit(0)
	}

	if err := cfg.DynamicUnmarshal(&LokiCfg, os.Args[1:], flag.CommandLine); err != nil {
		exitWithError("failed parsing config: %v\n", err)
	}

	// Set the global OTLP config which is needed in per tenant otlp config
	LokiCfg.LimitsConfig.SetGlobalOTLPConfig(LokiCfg.Distributor.OTLPConfig)
	// This global is set to the config passed into the last call to `NewOverrides`. If we don't
	// call it atleast once, the defaults are set to an empty struct.
	// We call it with the flag values so that the config file unmarshalling only overrides the values set in the config.
	validation.SetDefaultLimitsForYAMLUnmarshalling(LokiCfg.LimitsConfig)
	runtime.SetDefaultLimitsForYAMLUnmarshalling(LokiCfg.OperationalConfig)

	serverCfg := &LokiCfg.Server
	serverCfg.Log = logger

	if LokiCfg.InternalServer.Enable {
		LokiCfg.InternalServer.Log = serverCfg.Log
	}

	// Validate the config once both the config file has been loaded
	// and CLI flags parsed.
	if err := LokiCfg.Validate(); err != nil {
		level.Error(logger).Log("msg", "validating config", "err", err.Error())
		os.Exit(1)
	}

	if LokiCfg.PrintConfig {
		if err := util.PrintConfig(os.Stderr, &LokiCfg); err != nil {
			level.Error(logger).Log("msg", "failed to print config to stderr", "err", err.Error())
		}
	}

	if LokiCfg.LogConfig {
		if err := util.LogConfig(&LokiCfg); err != nil {
			level.Error(logger).Log("msg", "failed to log config object", "err", err.Error())
		}
	}

	if LokiCfg.VerifyConfig {
		level.Info(logger).Log("msg", "config is valid")
		os.Exit(0)
	}

	if LokiCfg.Tracing.Enabled {
		// Setting the environment variable JAEGER_AGENT_HOST enables tracing
		trace, err := tracing.NewFromEnv(fmt.Sprintf("loki-%s", LokiCfg.Target))
		if err != nil {
			level.Error(logger).Log("msg", "error in initializing tracing. tracing will not be enabled", "err", err)
		}
		if LokiCfg.Tracing.ProfilingEnabled {
			opentracing.SetGlobalTracer(spanprofiler.NewTracer(opentracing.GlobalTracer()))
		}
		defer func() {
			if trace != nil {
				if err := trace.Close(); err != nil {
					level.Error(logger).Log("msg", "error closing tracing", "err", err)
				}
			}
		}()
	}

	app, err := loki.New(LokiCfg.Config)
	if err != nil {
		level.Info(logger).Log("msg", "failed to initialize loki", "err", err)
		os.Exit(1)
	}

	_, err = app.ModuleManager.InitModuleServices("bloom-builder")
	if err != nil {
		level.Info(logger).Log("msg", "failed to initialize modules", "err", err)
		os.Exit(1)
	}

	level.Info(logger).Log("msg", "initialized loki")

	bounds := v1.NewBounds(model.Fingerprint(fp), model.Fingerprint(fp))

	tsdbIdentifier, ok := tsdb.ParseSingleTenantTSDBPath(envTSDBIdent)
	if !ok {
		level.Info(logger).Log("msg", "failed to parse TSDB path")
		os.Exit(1)
	}

	task := protos.NewTask(
		config.DayTable{
			DayTime: dayTime,
			Prefix:  app.Cfg.SchemaConfig.Configs[len(app.Cfg.SchemaConfig.Configs)-1].IndexTables.Prefix,
		},
		"29",
		bounds,
		tsdbIdentifier,
		[]protos.GapWithBlocks{
			{Bounds: bounds, Blocks: []bloomshipper.BlockRef{}},
		},
	)

	bb, err := builder.New(
		app.Cfg.BloomBuild.Builder,
		app.Overrides,
		app.Cfg.SchemaConfig,
		app.Cfg.StorageConfig,
		app.ClientMetrics,
		app.Store,
		app.BloomStore,
		logger,
		nil,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to create builder", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	metas, err := bb.Process(ctx, task)
	if err != nil {
		level.Info(logger).Log("msg", "failed to process task", "err", err)
		os.Exit(1)
	}

	fmt.Println("")
	fmt.Println("Created metas", metas)
	fmt.Println("DONE")
}
