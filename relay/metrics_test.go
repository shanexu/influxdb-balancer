package relay

import (
	"reflect"
	"testing"
)

func TestNewMeasurementKey(t *testing.T) {
	type args struct {
		name string
		tvs  []string
	}
	tests := []struct {
		name string
		args args
		want MeasurementKey
	}{
		{
			"case1",
			args{"m", nil},
			"m",
		},
		{
			"case2",
			args{"m", []string{"a"}},
			"m",
		},
		{
			"case3",
			args{"m", []string{"a", "b"}},
			"m a=b",
		},
		{
			"case4",
			args{"m", []string{"a", "b", "c", "d"}},
			"m a=b,c=d",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMeasurementKey(tt.args.name, tt.args.tvs...); got != tt.want {
				t.Errorf("NewMeasurementKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMeasurementKey_Measurement(t *testing.T) {
	tests := []struct {
		name string
		m    MeasurementKey
		want string
	}{
		{
			"case1",
			"m",
			"m",
		},
		{
			"case2",
			"m a=b",
			"m",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.Measurement(); got != tt.want {
				t.Errorf("MeasurementKey.Measurement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMeasurementKey_Tags(t *testing.T) {
	tests := []struct {
		name string
		m    MeasurementKey
		want map[string]string
	}{
		{
			"",
			"m",
			nil,
		},
		{
			"",
			"m ",
			map[string]string{},
		},
		{
			"",
			"m a=b",
			map[string]string{"a": "b"},
		},
		{
			"",
			"m a=b,c=d",
			map[string]string{"a": "b", "c":"d"},
		},
		{
			"",
			"m a=b,c=",
			map[string]string{"a": "b"},
		},
		{
			"",
			"m a=",
			map[string]string{},
		},
		{
			"",
			"m a",
			map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.Tags(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MeasurementKey.Tags() = %v, want %v", got, tt.want)
			}
		})
	}
}
