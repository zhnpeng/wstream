package types

import (
	"reflect"
	"testing"
	"time"
)

func TestNewMapRecord(t *testing.T) {
	type args struct {
		t time.Time
		v map[string]interface{}
	}
	now := time.Now()
	tests := []struct {
		name string
		args args
		want *MapRecord
	}{
		{
			name: "new map record",
			args: args{
				t: now,
				v: map[string]interface{}{
					"count": 1,
				},
			},
			want: &MapRecord{
				T: now,
				V: map[string]interface{}{
					"count": 1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMapRecord(tt.args.t, tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMapRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMapRecord_Funcs(t *testing.T) {
	now := time.Now()

	record := &MapRecord{
		V: make(map[string]interface{}),
	}

	record.SetTime(now)

	if !reflect.DeepEqual(record.Time(), now) {
		t.Errorf("record.T = %v, want %v", record.Time(), now)
	}

	k := []interface{}{"A", "B"}
	record.SetKey(k)
	if !reflect.DeepEqual(record.Key(), k) {
		t.Errorf("record.K = %v, want %v", record.Key(), k)
	}

	v := map[string]interface{}{
		"count": 1,
		"type":  "buy",
	}
	record.Set("count", 1)
	record.Set("type", "buy")
	if !reflect.DeepEqual(record.V, v) {
		t.Errorf("record.V = %v, want %v", record.V, v)
	}

	record.UseKeys("type")
	if !reflect.DeepEqual(record.Key(), []interface{}{"buy"}) {
		t.Errorf("record.K = %v, want %v", record.Key(), []interface{}{"buy"})
	}

	if record.Get("count") != 1 {
		t.Errorf("record.Get = %v, want %v", record.Get("count"), 1)
	}

	if !reflect.DeepEqual(record.GetMany("count", "type"), []interface{}{1, "buy"}) {
		t.Errorf("record.GetMany = %v, want %v", record.GetMany("count", "type"), []interface{}{1, "buy"})
	}

	if !reflect.DeepEqual(record, record.Clone()) {
		t.Errorf("record.Clone = %v, want %v", record.Clone(), record)
	}
}
