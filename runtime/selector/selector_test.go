package selector

import (
	"reflect"
	"testing"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/types"
)

type testSelect struct{}

func (t *testSelect) Select(record types.Record, size int) int {
	return 3 % size
}

func TestSelector_Select(t *testing.T) {
	type fields struct {
		function functions.Select
	}
	type args struct {
		record types.Record
		size   int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name: "",
			fields: fields{
				function: &testSelect{},
			},
			args: args{
				record: &types.MapRecord{
					V: map[string]interface{}{
						"type": "buy",
					},
				},
				size: 5,
			},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Selector{
				function: tt.fields.function,
			}
			if got := s.Select(tt.args.record, tt.args.size); got != tt.want {
				t.Errorf("Selector.Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSelector(t *testing.T) {
	type args struct {
		fn functions.Select
	}
	tests := []struct {
		name string
		args args
		want *Selector
	}{
		{
			name: "",
			args: args{
				fn: &testSelect{},
			},
			want: &Selector{
				function: &testSelect{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSelector(tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}
