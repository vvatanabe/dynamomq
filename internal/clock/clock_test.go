package clock

import (
	"strings"
	"testing"
	"time"
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
	testNow(t, Now())
}

func TestRealClockNow(t *testing.T) {
	c := RealClock{}
	testNow(t, c.Now())
}

func TestFormatRFC3339Nano(t *testing.T) {
	now := time.Now()
	formatted := FormatRFC3339Nano(now)
	if !strings.Contains(formatted, "T") {
		t.Errorf("FormatRFC3339Nano() did not format in RFC3339Nano")
	}
}

func TestRFC3339NanoToUnixMilli(t *testing.T) {
	now := time.Now().UTC()
	formatted := now.Format(time.RFC3339Nano)
	millis := RFC3339NanoToUnixMilli(formatted)
	if millis/1000 != now.Unix() {
		t.Errorf("RFC3339NanoToUnixMilli() did not convert to Unix milliseconds correctly")
	}
}
