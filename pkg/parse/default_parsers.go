package parse

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

func toInt(m map[string]interface{}, k string) (int64, bool) {
	v, ok := m[k]
	if !ok {
		return 0, false
	}
	floatVal, ok := v.(float64)
	if !ok {
		return 0, false
	}
	return int64(math.Floor(floatVal)), true
}

func float64AsTime(x float64) time.Time {
	return time.Unix(int64(math.Floor(x)), int64(1_000_000_000*(x-math.Floor(x))))
}

// StrictUnixTimeParser always treats the incoming data as a float64 number of seconds since the
// Unix epoch.
func StrictUnixTimeParser(in interface{}) (time.Time, error) {
	switch x := in.(type) {
	case int:
		return time.Unix(int64(x), 0), nil
	case int64:
		return time.Unix(x, 0), nil
	case float64:
		return float64AsTime(x), nil
	case string:
		raw, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("strict unix timestamp parser: cannot parse string %s into a float64: %v", x, err)
		}
		return float64AsTime(raw), nil
	default:
		return time.Time{}, fmt.Errorf("invalid time format %T(%v)", x, x)
	}
}

// DefaultTimeParser treats numbers as seconds since the Unix epoch and strings as RFC3339 timestamps.
func DefaultTimeParser(in interface{}) (time.Time, error) {
	switch x := in.(type) {
	case int:
		return time.Unix(int64(x), 0), nil
	case int64:
		return time.Unix(x, 0), nil
	case float64:
		return float64AsTime(x), nil
	case string:
		t, err := time.Parse(time.RFC3339, x)
		if err != nil {
			return time.Time{}, fmt.Errorf("interpreting string timestamp as RFC3339: %v", err)
		}
		return t, nil
	case map[string]interface{}: // logrus -> joonix Stackdriver format
		sec, sok := toInt(x, "seconds")
		nsec, nsok := toInt(x, "nanos")
		if !(sok && nsok) {
			return time.Time{}, fmt.Errorf("map[string]interface{}%v not in stackdriver format", x)
		}
		return time.Unix(sec, nsec), nil
	default:
		return time.Time{}, fmt.Errorf("invalid time format %T(%v)", x, x)
	}
}

// LagerLevelParser maps lager's float64 levels to log levels.
func LagerLevelParser(in interface{}) (Level, error) {
	x, ok := in.(float64)
	if !ok {
		return LevelUnknown, fmt.Errorf("invalid lager log level %T(%v), want float64", in, in)
	}
	switch x {
	case 0:
		return LevelDebug, nil
	case 1:
		return LevelInfo, nil
	case 2:
		return LevelError, nil
	case 3:
		return LevelFatal, nil
	default:
		return LevelUnknown, fmt.Errorf("invalid lager log level %v", x)
	}
}

// DefaultLevelParser uses common strings to determine the log level.  Case does not matter; info is
// the same log level as INFO.
func DefaultLevelParser(in interface{}) (Level, error) {
	var level string
	switch x := in.(type) {
	case string:
		level = x
	case []byte:
		level = string(x)
	default:
		return LevelUnknown, fmt.Errorf("invalid %T(%#v) for log level", in, in)
	}

	switch level {
	case "trace", "TRACE":
		return LevelTrace, nil
	case "debug", "DEBUG":
		return LevelDebug, nil
	case "info", "INFO":
		return LevelInfo, nil
	case "warn", "WARN":
		return LevelWarn, nil
	case "error", "ERROR":
		return LevelError, nil
	case "panic", "PANIC":
		return LevelPanic, nil
	case "dpanic", "DPANIC":
		return LevelDPanic, nil
	case "fatal", "FATAL":
		return LevelFatal, nil
	default:
		return LevelUnknown, nil
	}
}
