package objstore

import (
	"reflect"
	"testing"

	"github.com/grafana/loki/v3/pkg/storage"
)

func TestNew(t *testing.T) {
	type args struct {
		periodicConfigs []config.PeriodConfig
		storageConfig   storage.Config
		clientMetrics   storage.ClientMetrics
	}
	tests := []struct {
		name    string
		args    args
		want    *Multi
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.periodicConfigs, tt.args.storageConfig, tt.args.clientMetrics)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() got = %v, want %v", got, tt.want)
			}
		})
	}
}
