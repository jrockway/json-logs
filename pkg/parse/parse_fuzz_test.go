//go:build go1.18
// +build go1.18

package parse

import (
	"bufio"
	"bytes"
	"errors"
	"testing"

	"github.com/logrusorgru/aurora/v3"
)

func FuzzReadLog(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}\n`))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}\n` +
		`{"ts": 1235, "level": "warn", "msg": "line 2"}`))
	f.Add([]byte(`{"ts": 1234, "level": "info", "msg": "hello"}\n{}\n`))
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
				Aurora: aurora.NewAurora(true),
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
