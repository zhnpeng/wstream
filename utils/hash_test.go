package utils

import (
	"testing"
)

func TestHash(t *testing.T) {
	type args struct {
		bytes []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "test",
			args: args{
				bytes: []byte{'k', 'e', 'y'},
			},
			want: 3017358048, // well, this value is base on got
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Hash(tt.args.bytes); got != tt.want {
				t.Errorf("Hash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHashSlice(t *testing.T) {
	type args struct {
		slice []interface{}
	}
	tests := []struct {
		name string
		args args
		want KeyID
	}{
		{
			name: "test",
			args: args{
				slice: []interface{}{1, "buy"},
			},
			want: KeyID("[1 buy]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HashSlice(tt.args.slice); got != tt.want {
				t.Errorf("HashSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}
