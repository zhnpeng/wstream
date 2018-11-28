package assigners

import (
	"testing"
)

func TestGetWindowStartWithOffset(t *testing.T) {
	ts := int64(1543388461) // "2018-11-28T08 15:00:31"
	type args struct {
		ts     int64
		offset int64
		slide  int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "slide 60 seconds",
			args: args{
				ts:     ts,
				offset: 8,
				slide:  60,
			},
			want: 1543388460,
		},
		{
			name: "slide 60 seconds",
			args: args{
				ts:     ts,
				offset: 0,
				slide:  60,
			},
			want: 1543388460,
		},
		{
			name: "slide 70 seconds",
			args: args{
				ts:     ts,
				offset: 8,
				slide:  70,
			},
			want: 1543388450,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetWindowStartWithOffset(tt.args.ts, tt.args.offset, tt.args.slide); got != tt.want {
				t.Errorf("GetWindowStartWithOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}
