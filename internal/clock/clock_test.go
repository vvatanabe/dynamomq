package clock_test

import (
	"strings"
	"testing"
	"time"

	"github.com/vvatanabe/dynamomq/internal/clock"
)

func testNow(t *testing.T, now time.Time) {
	if now.IsZero() {
		t.Errorf("Now() returned zero time")
	}
	if !strings.HasSuffix(now.String(), "UTC") {
		t.Errorf("Now() did not return UTC time")
	}
}

func TestNow(t *testing.T) {
	testNow(t, clock.Now())
}

func TestRealClockNow(t *testing.T) {
	c := clock.RealClock{}
	testNow(t, c.Now())
}

func TestFormatRFC3339Nano(t *testing.T) {
	now := time.Now()
	formatted := clock.FormatRFC3339Nano(now)
	if !strings.Contains(formatted, "T") {
		t.Errorf("FormatRFC3339Nano() did not format in RFC3339Nano")
	}
}

func TestRFC3339NanoToUnixMilli(t *testing.T) {
	now := time.Now().UTC()
	formatted := now.Format(time.RFC3339Nano)
	millis := clock.RFC3339NanoToUnixMilli(formatted)
	if millis/1000 != now.Unix() {
		t.Errorf("RFC3339NanoToUnixMilli() did not convert to Unix milliseconds correctly")
	}
}
