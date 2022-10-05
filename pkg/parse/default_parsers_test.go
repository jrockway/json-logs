package parse

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap/zapcore"
)

func TestTimeParsers(t *testing.T) {
	testData := []struct {
		in      interface{}
		parser  TimeParser
		want    time.Time
		wantErr bool
	}{
		{int(1), DefaultTimeParser, time.Unix(1, 0), false},
		{int(1), StrictUnixTimeParser, time.Unix(1, 0), false},
		{int64(1), DefaultTimeParser, time.Unix(1, 0), false},
		{int64(1), StrictUnixTimeParser, time.Unix(1, 0), false},
		{float64(1.1), DefaultTimeParser, time.Unix(1, 100000000), false},
		{float64(1.1), StrictUnixTimeParser, time.Unix(1, 100000000), false},
		{"1.1", StrictUnixTimeParser, time.Unix(1, 100000000), false},
		{"foo", StrictUnixTimeParser, time.Time{}, true},
		{"1970-01-01T00:00:01.000Z", DefaultTimeParser, time.Unix(1, 0), false},
		{"1970-01-01T04:00:01.000+04:00", DefaultTimeParser, time.Unix(1, 0), false},
		{map[string]interface{}{"seconds": float64(123), "nanos": float64(456)}, DefaultTimeParser, time.Unix(123, 456), false},
		{map[string]interface{}{"seconds": "123", "nanos": "456"}, DefaultTimeParser, time.Time{}, true},
		{map[string]interface{}{"garbage": float64(123), "trash": float64(456)}, DefaultTimeParser, time.Time{}, true},
		{nil, DefaultTimeParser, time.Time{}, true},
		{nil, StrictUnixTimeParser, time.Time{}, true},
		{"1", DefaultTimeParser, time.Time{}, true},
		{"1", StrictUnixTimeParser, time.Unix(1, 0), false},
	}
	for i, test := range testData {
		got, err := test.parser(test.in)
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

func TestLevelParsers(t *testing.T) {
	testData := []struct {
		in      interface{}
		parser  LevelParser
		want    Level
		wantErr bool
	}{
		{"trace", DefaultLevelParser, LevelTrace, false},
		{"TRACE", DefaultLevelParser, LevelTrace, false},
		{zapcore.DebugLevel.CapitalString(), DefaultLevelParser, LevelDebug, false},
		{zapcore.DebugLevel.String(), DefaultLevelParser, LevelDebug, false},
		{[]byte(zapcore.DebugLevel.CapitalString()), DefaultLevelParser, LevelDebug, false},
		{zapcore.InfoLevel.CapitalString(), DefaultLevelParser, LevelInfo, false},
		{zapcore.InfoLevel.String(), DefaultLevelParser, LevelInfo, false},
		{zapcore.WarnLevel.CapitalString(), DefaultLevelParser, LevelWarn, false},
		{zapcore.WarnLevel.String(), DefaultLevelParser, LevelWarn, false},
		{zapcore.ErrorLevel.CapitalString(), DefaultLevelParser, LevelError, false},
		{zapcore.ErrorLevel.String(), DefaultLevelParser, LevelError, false},
		{zapcore.PanicLevel.CapitalString(), DefaultLevelParser, LevelPanic, false},
		{zapcore.PanicLevel.String(), DefaultLevelParser, LevelPanic, false},
		{zapcore.FatalLevel.CapitalString(), DefaultLevelParser, LevelFatal, false},
		{zapcore.FatalLevel.String(), DefaultLevelParser, LevelFatal, false},
		{zapcore.DPanicLevel.CapitalString(), DefaultLevelParser, LevelDPanic, false},
		{zapcore.DPanicLevel.String(), DefaultLevelParser, LevelDPanic, false},
		{"foo", DefaultLevelParser, LevelUnknown, false},
		{"iNfO", DefaultLevelParser, LevelUnknown, false},
		{42, DefaultLevelParser, LevelUnknown, true},
		{float64(-1), LagerLevelParser, LevelUnknown, true},
		{float64(0), LagerLevelParser, LevelDebug, false},
		{int(0), LagerLevelParser, LevelUnknown, true},
		{float64(1), LagerLevelParser, LevelInfo, false},
		{float64(2), LagerLevelParser, LevelError, false},
		{float64(3), LagerLevelParser, LevelFatal, false},
		{float64(10), BunyanV0LevelParser, LevelTrace, false},
		{float64(20), BunyanV0LevelParser, LevelDebug, false},
		{float64(30), BunyanV0LevelParser, LevelInfo, false},
		{float64(40), BunyanV0LevelParser, LevelWarn, false},
		{float64(50), BunyanV0LevelParser, LevelError, false},
		{float64(60), BunyanV0LevelParser, LevelFatal, false},
		{"foo", BunyanV0LevelParser, LevelUnknown, true},
		{float64(61), BunyanV0LevelParser, LevelUnknown, true},
	}
	for i, test := range testData {
		got, err := test.parser(test.in)
		if err != nil && !test.wantErr {
			t.Errorf("test %d: unexpected error: %v", i, err)
		} else if err == nil && test.wantErr {
			t.Errorf("test %d: expected error", i)
		}
		if want := test.want; got != want {
			t.Errorf("test %d: level:\n  got: %v, want: %v", i, got, want)
		}
	}
}

func TestNoopParsers(t *testing.T) {
	//nolint: errcheck
	testData := []func(){func() { NoopTimeParser(1) }, func() { NoopLevelParser("info") }}
	for _, test := range testData {
		ok := func() (ok bool) {
			defer func() {
				ok = recover() != nil
			}()
			test()
			return false
		}()
		if !ok {
			t.Error("expected panic")
		}
	}
}
