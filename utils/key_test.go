package utils

import (
	"testing"
)

func TestPartitionByKeys(t *testing.T) {
	type args struct {
		count int
		data  []interface{}
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{

			name: "count = 5",
			args: args{
				count: 5,
				data:  []interface{}{1, "buy", []byte{'a'}},
			},
			want: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PartitionByKeys(tt.args.count, tt.args.data); got != tt.want {
				t.Errorf("PartitionByKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLessThan(t *testing.T) {
	type args struct {
		a interface{}
		b interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "string",
			args: args{
				a: "aaa",
				b: "bbb",
			},
			want: true,
		},
		{
			name: "[]byte",
			args: args{
				a: []byte{'a', 'a'},
				b: []byte{'b', 'b'},
			},
			want: true,
		},
		{
			name: "int",
			args: args{
				a: 1,
				b: 2,
			},
			want: true,
		},
		{
			name: "[]interface{}",
			args: args{
				a: []interface{}{"a"},
				b: []interface{}{"b"},
			},
			want: true,
		},
		{
			name: "negative int",
			args: args{
				a: -2,
				b: -1,
			},
			want: true,
		},
		{
			name: "float64 grater",
			args: args{
				a: float64(5.5),
				b: float64(5.49),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LessThan(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("LessThan() = %v, want %v", got, tt.want)
			}
		})
	}
}
