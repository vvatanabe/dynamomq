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
			name: "should return StatusReady when VisibilityTimeout is 0",
			m: dynamomq.Message[any]{
				VisibilityTimeout:      0,
				PeekFromQueueTimestamp: clock.FormatRFC3339Nano(test.DefaultTestDate),
			},
			args: args{
				now: test.DefaultTestDate,
			},
			want: dynamomq.StatusReady,
		},
		{
			name: "should return StatusProcessing when current time is before VisibilityTimeout",
			m: dynamomq.Message[any]{
				VisibilityTimeout:      1,
				PeekFromQueueTimestamp: clock.FormatRFC3339Nano(test.DefaultTestDate.Add(time.Second)),
			},
			args: args{
				now: test.DefaultTestDate.Add(time.Second),
			},
			want: dynamomq.StatusProcessing,
		},
		{
			name: "should return StatusReady when current time is after VisibilityTimeout",
			m: dynamomq.Message[any]{
				VisibilityTimeout:      5,
				PeekFromQueueTimestamp: clock.FormatRFC3339Nano(test.DefaultTestDate),
			},
			args: args{
				now: test.DefaultTestDate.Add(time.Second * 6),
			},
			want: dynamomq.StatusReady,
		},
		{
			name: "should return StatusProcessing when current time is equal VisibilityTimeout",
			m: dynamomq.Message[any]{
				VisibilityTimeout:      4,
				PeekFromQueueTimestamp: clock.FormatRFC3339Nano(test.DefaultTestDate.Add(time.Second * 4)),
			},
			args: args{
				now: time.Date(2021, 1, 1, 0, 0, 4, 0, time.UTC),
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
