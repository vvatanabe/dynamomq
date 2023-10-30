package clock

import "time"

func Now() time.Time {
	return time.Now().UTC()
}

func FormatRFC3339Nano(now time.Time) string {
	return now.UTC().Format(time.RFC3339Nano)
}

func RFC3339NanoToUnixMilli(rfc3339NanoDate string) int64 {
	t, _ := time.Parse(time.RFC3339Nano, rfc3339NanoDate)
	return t.UnixMilli()
}

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (m RealClock) Now() time.Time {
	return Now()
}
