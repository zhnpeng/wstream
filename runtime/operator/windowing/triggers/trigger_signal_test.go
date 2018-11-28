package triggers

import (
	"testing"
)

func TestTriggerSignal_String(t *testing.T) {
	tests := []struct {
		name string
		s    TriggerSignal
		want string
	}{
		{
			name: "Fire",
			s:    FIRE,
			want: "Fire",
		},
		{
			name: "Continue",
			s:    CONTINUE,
			want: "Continue",
		},
		{
			name: "Invalid Singal",
			s:    TriggerSignal(-1),
			want: "Invalid Signal",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.String(); got != tt.want {
				t.Errorf("TriggerSignal.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTriggerSignal_IsFire(t *testing.T) {
	tests := []struct {
		name string
		s    TriggerSignal
		want bool
	}{
		{
			name: "Fire",
			s:    FIRE,
			want: true,
		},
		{
			name: "Continue",
			s:    CONTINUE,
			want: false,
		},
		{
			name: "Invalid Signal",
			s:    TriggerSignal(-1),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.IsFire(); got != tt.want {
				t.Errorf("TriggerSignal.IsFire() = %v, want %v", got, tt.want)
			}
		})
	}
}
