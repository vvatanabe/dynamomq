package clock

import "time"

func Now() time.Time {
	return time.Now().UTC()
}

func FormatRFC3339(now time.Time) string {
	return now.UTC().Format(time.RFC3339)
}

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (m RealClock) Now() time.Time {
	return Now()
}
