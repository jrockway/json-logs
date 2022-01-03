//go:build go1.18
// +build go1.18

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

	"github.com/logrusorgru/aurora/v3"
)

func FuzzReadLog(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}` + "\n"))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}` + "\n" +
		`{"ts": 1235, "level": "warn", "msg": "line 2"}`))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}` + "\n{}\n"))
	jq, err := CompileJQ(".")
	if err != nil {
		f.Fatalf("jq: %v", err)
	}

	f.Fuzz(func(t *testing.T, in []byte) {
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
		if _, err := ReadLog(inbuf, outbuf, ins, outs, jq); err != nil {
			if errors.Is(err, bufio.ErrTooLong) {
				// This is a known limit and the fuzzer likes to produce very long
				// garbage lines.  The tests convinced me to increase this limit,
				// but it has to be limited somewhere.
				t.SkipNow()
			}
			t.Fatal(err)
		}
		if got, want := bytes.Count(outbuf.Bytes(), []byte("\n")), bytes.Count(in, []byte("\n")); got < want {
			t.Errorf("output: line count:\n  got:   %v\n want: >=%v", got, want)
		}
		if errbuf.Len() > 0 {
			t.Logf("errors: %v", errbuf.String())
		}
	})
}

func FuzzEmit(f *testing.F) {
	f.Add(1.0, 1, "hello", false, "key\nvalue\nkey2\nvalue2", "America/New_York", false, time.RFC3339)
	f.Fuzz(func(t *testing.T, ts float64, lvl int, msg string, highlight bool, fields string, zone string, elideDuplicate bool, timeFormat string) {
		tz, err := time.LoadLocation(zone)
		if err != nil {
			tz = time.Local
		}
		fieldMap := map[string]interface{}{}
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
		if _, err := DefaultLevelParser(interface{}(in)); err != nil {
			t.Fatal(err)
		}
	})
}

func prepareTime(in string) interface{} {
	if len(in) > 0 && in[0] == '{' {
		result := make(map[string]interface{})
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
		DefaultTimeParser(prepareTime(in))
	})
}

func FuzzStrictUnixTimeParser(f *testing.F) {
	f.Add("1641092371")
	f.Add("1641092371.456")
	f.Fuzz(func(t *testing.T, in string) {
		// All we care about are panics.  Errors are expected.
		StrictUnixTimeParser(prepareTime(in))
	})
}
