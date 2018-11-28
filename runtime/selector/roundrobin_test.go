package selector

import (
	"reflect"
	"testing"

	"github.com/wandouz/wstream/types"
)

func TestRoundRobinSelector_Select(t *testing.T) {
	type fields struct {
		count int64
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
				count: 5,
			},
			args: args{
				record: &types.MapRecord{
					V: map[string]interface{}{
						"type": "buy",
					},
				},
				size: 5,
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &RoundRobinSelector{
				count: tt.fields.count,
			}
			if got := m.Select(tt.args.record, tt.args.size); got != tt.want {
				t.Errorf("RoundRobinSelector.Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewRoundRobinSelector(t *testing.T) {
	tests := []struct {
		name string
		want *RoundRobinSelector
	}{
		{
			name: "",
			want: &RoundRobinSelector{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewRoundRobinSelector(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewRoundRobinSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}
