package selector

import (
	"reflect"
	"testing"

	"github.com/zhnpeng/wstream/types"
)

func TestKeyBy_Select(t *testing.T) {
	type fields struct {
		keys []interface{}
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
				keys: []interface{}{"type"},
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
			m := &KeyBy{
				keys: tt.fields.keys,
			}
			if got := m.Select(tt.args.record, tt.args.size); got != tt.want {
				t.Errorf("KeyBy.Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewKeyBy(t *testing.T) {
	type args struct {
		keys []interface{}
	}
	tests := []struct {
		name string
		args args
		want *KeyBy
	}{
		{
			name: "",
			args: args{
				keys: []interface{}{"type"},
			},
			want: &KeyBy{
				keys: []interface{}{"type"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewKeyBy(tt.args.keys); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewKeyBy() = %v, want %v", got, tt.want)
			}
		})
	}
}
