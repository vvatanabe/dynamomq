package dynamomq_test

import (
	"testing"
	"time"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func TestMessageGetStatus(t *testing.T) {
	type args struct {
		now time.Time
	}
	type testCase[T any] struct {
		name string
		m    dynamomq.Message[T]
		args args
		want dynamomq.Status
	}
	tests := []testCase[any]{
		{
			name: "should return StatusReady when InvisibleUntilAt is empty",
			m: dynamomq.Message[any]{
				InvisibleUntilAt: "",
			},
			args: args{
				now: test.DefaultTestDate,
			},
			want: dynamomq.StatusReady,
		},
		{
			name: "should return StatusProcessing when current time is before InvisibleUntilAt",
			m: dynamomq.Message[any]{
				InvisibleUntilAt: clock.FormatRFC3339Nano(test.DefaultTestDate.Add(time.Second)),
			},
			args: args{
				now: test.DefaultTestDate,
			},
			want: dynamomq.StatusProcessing,
		},
		{
			name: "should return StatusReady when current time is after InvisibleUntilAt",
			m: dynamomq.Message[any]{
				InvisibleUntilAt: clock.FormatRFC3339Nano(test.DefaultTestDate.Add(time.Second * 5)),
			},
			args: args{
				now: test.DefaultTestDate.Add(time.Second * 6),
			},
			want: dynamomq.StatusReady,
		},
		{
			name: "should return StatusProcessing when current time is equal InvisibleUntilAt",
			m: dynamomq.Message[any]{
				InvisibleUntilAt: clock.FormatRFC3339Nano(test.DefaultTestDate),
			},
			args: args{
				now: test.DefaultTestDate,
			},
			want: dynamomq.StatusProcessing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.GetStatus(tt.args.now); got != tt.want {
				t.Errorf("GetStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}
