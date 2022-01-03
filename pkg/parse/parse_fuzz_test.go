package parse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/itchyny/gojq"
	"github.com/jrockway/json-logs/pkg/parse/internal/fuzzsupport"
	"github.com/logrusorgru/aurora/v3"
)

// runReadLog runs ReadLog against some input, and asserts that certain expectations are met.  It's
// used to implement FuzzReadLogs and FuzzReadLogsWithJSON.
func runReadLog(t *testing.T, jq *gojq.Code, in []byte, expectedLines int) {
	t.Helper()
	inbuf := bytes.NewReader(in)
	ins := &InputSchema{
		Strict: false,
	}
	errbuf := new(bytes.Buffer)
	outs := &OutputSchema{
		Formatter: &DefaultOutputFormatter{
			Aurora:             aurora.NewAurora(true),
			AbsoluteTimeFormat: time.RFC3339,
			Zone:               time.Local,
		},
		EmitErrorFn: func(msg string) {
			errbuf.WriteString(msg)
			errbuf.WriteString("\n")
		},
	}
	outbuf := new(bytes.Buffer)
	summary, err := ReadLog(inbuf, outbuf, ins, outs, jq)
	if err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			// This is a known limit and the fuzzer likes to produce very long
			// garbage lines.  The tests convinced me to increase this limit,
			// but it has to be limited somewhere.
			t.SkipNow()
		}
		t.Fatal(err)
	}
	outBytes := outbuf.Bytes()
	approxInputLines := bytes.Count(in, []byte("\n"))
	if got, want := summary.Lines, approxInputLines; got < want {
		t.Errorf("input line count compared to summary:\n  got: %v\n want: %v", got, want)
	}
	gotOutputLines := bytes.Count(outBytes, []byte("\n"))
	if got, want := gotOutputLines, approxInputLines; got < want {
		t.Errorf("output line count:\n  got:   %v\n want: >=%v", got, want)
	}
	if expectedLines > 0 {
		if got, want := summary.Lines, expectedLines; got != want {
			t.Errorf("summary: exact expected line count:\n  got: %v\n want: %v", got, want)
		}
	}
	if errbuf.Len() > 0 {
		t.Logf("errors: %v", errbuf.String())
	}
}

// FuzzReadLog fuzzes ReadLog with random binary input.
func FuzzReadLog(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}` + "\n"))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}` + "\n" +
		`{"ts": 1235, "level": "warn", "msg": "line 2"}`))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}` + "\n{}\n"))

	f.Fuzz(func(t *testing.T, in []byte) {
		runReadLog(t, nil, in, 0)
	})
}

// FuzzReadLogWithJSON attempts to turn a long string of arbitrary bytes into a reasonable stream of
// JSON logs.  This lets the fuzzer more quickly get into interesting cases than the above
// FuzzReadLog, which mostly exercises cases where the JSON doesn't even parse.
//
// The example corpus is taken from internal/fuzzsupport/generator_test#TestUnmarshalText.  If you
// add an example here, also add it to that test so you're sure of what it does.
func FuzzReadLogWithJSON(f *testing.F) {
	f.Add("")
	f.Add("\x00\x00\x00\x00")
	f.Add("\x01\x04\x07")
	f.Add("\x01\x04\x07\xfffoo\x00bar\x00\x00\x01\x04\x07")

	f.Fuzz(func(t *testing.T, in string) {
		var l fuzzsupport.JSONLogs
		if err := l.UnmarshalText([]byte(in)); err != nil {
			t.Fatalf("unmarshal test case: %v", err)
		}
		runReadLog(t, nil, l.Data, l.NLines)
	})
}

func FuzzEmit(f *testing.F) {
	f.Add(1.0, 1, "hello", false, "key\nvalue\nkey2\nvalue2", "America/New_York", false, time.RFC3339)
	f.Fuzz(func(t *testing.T, ts float64, lvl int, msg string, highlight bool, fields string, zone string, elideDuplicate bool, timeFormat string) {
		tz, err := time.LoadLocation(zone)
		if err != nil {
			tz = time.Local
		}
		fieldMap := map[string]any{}
		parts := strings.Split(fields, "\n")
		for i := 0; i+1 < len(parts); i += 2 {
			fieldMap[parts[i]] = parts[i+1]
		}
		outs := &OutputSchema{
			PriorityFields: []string{"0"},
			Formatter: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(true),
				ElideDuplicateFields: elideDuplicate,
				AbsoluteTimeFormat:   timeFormat,
				Zone:                 tz,
			},
			state: State{
				seenFields: []string{"a"},
				lastTime:   time.Unix(0, 0),
				lastFields: make(map[string][]byte),
			},
		}
		l := &line{
			time:      float64AsTime(ts),
			lvl:       Level(lvl),
			msg:       msg,
			highlight: highlight,
			fields:    fieldMap,
		}
		outbuf := new(bytes.Buffer)
		outs.Emit(l, outbuf)
		byts := outbuf.Bytes()
		if len(byts) == 0 {
			t.Fatal("no output produced")
		}
		if byts[len(byts)-1] != '\n' {
			t.Fatal("no trailing newline")
		}
		wantMsg := strings.Replace(msg, "\n", "↩", -1)
		if !bytes.Contains(byts, []byte(wantMsg)) {
			t.Fatal("message not in output")
		}
	})
}

func FuzzDefaultLevelParser(f *testing.F) {
	f.Add("info")
	f.Fuzz(func(t *testing.T, in string) {
		if _, err := DefaultLevelParser(any(in)); err != nil {
			t.Fatal(err)
		}
	})
}

func prepareTime(in string) any {
	if len(in) > 0 && in[0] == '{' {
		result := make(map[string]any)
		if err := json.Unmarshal([]byte(in), &result); err == nil {
			return result
		}
	}
	f, err := strconv.ParseFloat(in, 64)
	if err == nil {
		if f-math.Floor(f) < 0.00000000001 {
			return int64(f)
		} else {
			return f
		}
	}
	return in
}

func FuzzDefaultTimeParser(f *testing.F) {
	f.Add("1641092371")
	f.Add("1641092371.456")
	f.Add("2020-01-01T01:02:03.456")
	f.Add(`{"seconds": 42, "nanos": 69}`)
	f.Fuzz(func(t *testing.T, in string) {
		// All we care about are panics.  Errors are expected.
		DefaultTimeParser(prepareTime(in)) // nolint:errcheck
	})
}

func FuzzStrictUnixTimeParser(f *testing.F) {
	f.Add("1641092371")
	f.Add("1641092371.456")
	f.Fuzz(func(t *testing.T, in string) {
		// All we care about are panics.  Errors are expected.
		StrictUnixTimeParser(prepareTime(in)) // nolint:errcheck
	})
}
