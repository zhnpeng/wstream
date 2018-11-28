package types

import (
	"reflect"
	"testing"
	"time"
)

func TestNewTupleRecord(t *testing.T) {
	type args struct {
		t time.Time
		v []interface{}
	}
	now := time.Now()
	tests := []struct {
		name string
		args args
		want *TupleRecord
	}{
		{
			name: "test new tuple record",
			args: args{
				t: now,
				v: []interface{}{1, "buy"},
			},
			want: &TupleRecord{
				T: now,
				V: []interface{}{1, "buy"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTupleRecord(tt.args.t, tt.args.v...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTupleRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTupleRecord_Funcs(t *testing.T) {
	now := time.Now()

	record := &TupleRecord{}

	record.SetTime(now)

	if !reflect.DeepEqual(record.Time(), now) {
		t.Errorf("record.T = %v, want %v", record.Time(), now)
	}

	k := []interface{}{"A", "B"}
	record.SetKey(k)
	if !reflect.DeepEqual(record.Key(), k) {
		t.Errorf("record.K = %v, want %v", record.Key(), k)
	}

	v := []interface{}{1, "buy"}
	record.Set(-1, 1)
	record.Set(-1, "sell")
	record.Set(1, "buy")
	if !reflect.DeepEqual(record.V, v) {
		t.Errorf("record.V = %v, want %v", record.V, v)
	}

	record.UseKeys(1)
	if !reflect.DeepEqual(record.Key(), []interface{}{"buy"}) {
		t.Errorf("record.K = %v, want %v", record.Key(), []interface{}{"buy"})
	}

	if record.Get(0) != 1 {
		t.Errorf("record.Get = %v, want %v", record.Get(0), 1)
	}

	if !reflect.DeepEqual(record.GetMany(0, 1), []interface{}{1, "buy"}) {
		t.Errorf("record.GetMany = %v, want %v", record.GetMany(0, 1), []interface{}{1, "buy"})
	}

	if !reflect.DeepEqual(record, record.Clone()) {
		t.Errorf("record.Clone = %v, want %v", record.Clone(), record)
	}

	if record.Type() != TypeTupleRecord {
		t.Errorf("record.Type = %v, want %v", record.Type(), TypeTupleRecord)
	}

	if _, err := record.AsRow(); err != nil {
		t.Error(err)
	}
}
