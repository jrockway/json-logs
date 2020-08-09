package parse

import (
	"errors"
	"fmt"
	"math"
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

func DefaultTimeParser(in interface{}) (time.Time, error) {
	var sec, nsec int64
	switch x := in.(type) {
	case int:
		sec = int64(x)
		nsec = 0
	case int64:
		sec = x
		nsec = 0
	case float64: // zap
		sec = int64(math.Floor(x))
		nsec = int64(1_000_000_000 * (x - math.Floor(x)))
	case string:
		t, err := time.Parse(time.RFC3339, x)
		if err != nil {
			return time.Time{}, fmt.Errorf("interpreting string timestamp as RFC3339: %v", err)
		}
		return t, nil
	case map[string]interface{}: // logrus -> joonix Stackdriver format
		s, sok := toInt(x, "seconds")
		if sok {
			sec = s
		}
		ns, nsok := toInt(x, "nanos")
		if nsok {
			nsec = ns
		}
		if !(sok && nsok) {
			return time.Time{}, errors.New("map[string]interface{} not in joonix Stackdriver format")
		}
	default:
		return time.Time{}, errors.New("invalid time format")
	}
	return time.Unix(sec, nsec), nil
}
