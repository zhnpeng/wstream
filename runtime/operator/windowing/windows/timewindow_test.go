package windows

import (
	"reflect"
	"testing"
	"time"

	"github.com/zhnpeng/wstream/utils"
)

func TestNewTimeWindow(t *testing.T) {
	start := utils.ParseTime("2018-11-28 16:00:00")
	end := utils.ParseTime("2018-11-28 16:01:00")
	type args struct {
		start time.Time
		end   time.Time
	}
	tests := []struct {
		name string
		args args
		want Window
	}{
		{
			name: "",
			args: args{
				start: start,
				end:   end,
			},
			want: Window{
				start: start,
				end:   end,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTimeWindow(tt.args.start, tt.args.end); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTimeWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}
