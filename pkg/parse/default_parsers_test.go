package parse

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap/zapcore"
)

func TestDefaultTimeParser(t *testing.T) {
	testData := []struct {
		in      interface{}
		want    time.Time
		wantErr bool
	}{
		{int(1), time.Unix(1, 0), false},
		{int64(1), time.Unix(1, 0), false},
		{float64(1.1), time.Unix(1, 100000000), false},
		{"1970-01-01T00:00:01.000Z", time.Unix(1, 0), false},
		{"1970-01-01T04:00:01.000+04:00", time.Unix(1, 0), false},
		{map[string]interface{}{"seconds": float64(123), "nanos": float64(456)}, time.Unix(123, 456), false},
		{map[string]interface{}{"seconds": "123", "nanos": "456"}, time.Time{}, true},
		{map[string]interface{}{"garbage": float64(123), "trash": float64(456)}, time.Time{}, true},
		{nil, time.Time{}, true},
		{"1", time.Time{}, true},
	}
	for i, test := range testData {
		got, err := DefaultTimeParser(test.in)
		if err != nil && !test.wantErr {
			t.Errorf("test %d: unexpected error: %v", i, err)
		} else if err == nil && test.wantErr {
			t.Errorf("test %d: expected error", i)
		}
		if diff := cmp.Diff(got, test.want, cmpopts.EquateApproxTime(time.Nanosecond)); diff != "" {
			t.Errorf("test %d: %s", i, diff)
		}
	}
}

func TestDefaultLevelParser(t *testing.T) {
	testData := []struct {
		in      interface{}
		want    Level
		wantErr bool
	}{
		{"tRaCe", LevelTrace, false},
		{zapcore.DebugLevel.CapitalString(), LevelDebug, false},
		{[]byte(zapcore.DebugLevel.CapitalString()), LevelDebug, false},
		{zapcore.InfoLevel.CapitalString(), LevelInfo, false},
		{zapcore.WarnLevel.CapitalString(), LevelWarn, false},
		{zapcore.ErrorLevel.CapitalString(), LevelError, false},
		{zapcore.PanicLevel.CapitalString(), LevelPanic, false},
		{zapcore.FatalLevel.CapitalString(), LevelFatal, false},
		{zapcore.DPanicLevel.CapitalString(), LevelDPanic, false},
		{"foo", LevelUnknown, false},
		{42, LevelUnknown, true},
	}
	for i, test := range testData {
		got, err := DefaultLevelParser(test.in)
		if err != nil && !test.wantErr {
			t.Errorf("test %d: unexpected error: %v", i, err)
		} else if err == nil && test.wantErr {
			t.Errorf("test %d: expected error", i)
		}
		if diff := cmp.Diff(got, test.want); diff != "" {
			t.Errorf("test %d: %s", i, diff)
		}
	}
}
