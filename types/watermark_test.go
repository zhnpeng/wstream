package types

import (
	"reflect"
	"testing"
	"time"
)

func TestNewWatermark(t *testing.T) {
	now := time.Now()
	type args struct {
		t time.Time
	}
	tests := []struct {
		name string
		args args
		want *Watermark
	}{
		{
			name: "test new watermark",
			args: args{
				t: now,
			},
			want: NewWatermark(now),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewWatermark(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewWatermark() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWatermark_Funcs(t *testing.T) {
	now := time.Now()

	record := &Watermark{}

	record.SetTime(now)

	if !reflect.DeepEqual(record.Time(), now) {
		t.Errorf("record.T = %v, want %v", record.Time(), now)
	}

	if !reflect.DeepEqual(record, record.Clone()) {
		t.Errorf("record.Clone = %v, want %v", record.Clone(), record)
	}

	if NewWatermark(now.Add(time.Second)).After(record) == false {
		t.Errorf("After =%v, want %v", false, true)
	}

	if record.Type() != TypeWatermark {
		t.Errorf("record.Type = %v, want %v", record.Type(), TypeWatermark)
	}

	if _, err := record.AsRow(); err != nil {
		t.Error(err)
	}
}
