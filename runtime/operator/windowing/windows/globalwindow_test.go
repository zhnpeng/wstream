package windows

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestGetGlobalWindow(t *testing.T) {
	tests := []struct {
		name string
		want Window
	}{
		{
			name: "",
			want: Window{
				end: time.Unix(math.MaxInt64, 0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetGlobalWindow(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetGlobalWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}
