package parse

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/logrusorgru/aurora/v3"
)

var defaultTime = time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)

func TestFormatting(t *testing.T) {
	programStartTime = defaultTime.Add(2*time.Hour + 3*time.Minute + 4*time.Second + 500*time.Millisecond + 600*time.Microsecond)
	testData := []struct {
		f    *DefaultOutputFormatter
		t    []time.Time
		want string
	}{
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: false,
				AbsoluteTimeFormat:   time.RFC3339,
				Zone:                 time.UTC,
			},
			t:    []time.Time{defaultTime},
			want: `2000-01-02T03:04:05Z INFO  hello↩world a:field b:{"nesting":"is real"}` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: false,
				AbsoluteTimeFormat:   time.RFC3339,
				Zone:                 time.UTC,
				HighlightFields:      map[string]struct{}{"a": {}},
			},
			t:    []time.Time{defaultTime},
			want: `2000-01-02T03:04:05Z INFO  hello↩world a:field b:{"nesting":"is real"}` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   time.RFC3339,
				Zone:                 time.UTC,
			},
			t:    []time.Time{defaultTime},
			want: `2000-01-02T03:04:05Z INFO  hello↩world a:field b:↑` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   time.RFC3339,
				Zone:                 time.UTC,
			},
			t:    []time.Time{time.Time{}},
			want: `       ??? INFO  hello↩world a:field b:↑` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
				Zone:                 time.UTC,
			},
			t:    []time.Time{defaultTime},
			want: `-2h3m4s    INFO  hello↩world a:field b:↑` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
				Zone:                 time.UTC,
			},
			t:    []time.Time{programStartTime.Add(-123)},
			want: `-123ns     INFO  hello↩world a:field b:↑` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
				Zone:                 time.UTC,
			},
			t:    []time.Time{programStartTime.Add(-123456)},
			want: `-123µs     INFO  hello↩world a:field b:↑` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
				Zone:                 time.UTC,
			},
			t:    []time.Time{programStartTime.Add(-123456789)},
			want: `-123ms     INFO  hello↩world a:field b:↑` + "\n",
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "03:04:05.000Z07:00",
				SubSecondsOnlyFormat: "        .000",
				Zone:                 time.UTC,
			},
			t: []time.Time{
				defaultTime,
				defaultTime.Add(123 * time.Millisecond),
				defaultTime.Add(999*time.Millisecond + 999*time.Microsecond + 999*time.Nanosecond), // note the rounding!
				defaultTime.Add(1001 * time.Millisecond),
				defaultTime.Add(1123 * time.Millisecond),
				defaultTime.Add(1456 * time.Millisecond),
				defaultTime.Add(2000 * time.Millisecond),
				defaultTime.Add(5*time.Second + 1455*time.Millisecond),
			},
			want: strings.Join([]string{
				`03:04:05.000Z INFO  hello↩world a:field b:↑`,
				`        .123  INFO  hello↩world a:↑ b:↑`,
				`        .999  INFO  hello↩world a:↑ b:↑`,
				`03:04:06.001Z INFO  hello↩world a:↑ b:↑`,
				`        .123  INFO  hello↩world a:↑ b:↑`,
				`        .456  INFO  hello↩world a:↑ b:↑`,
				`03:04:07.000Z INFO  hello↩world a:↑ b:↑`,
				`03:04:11.455Z INFO  hello↩world a:↑ b:↑`,
			}, "\n") + "\n",
		},
	}

	for _, test := range testData {
		var s State
		s.lastFields = map[string][]byte{"b": []byte(`{"nesting":"is real"}`)}
		s.timePadding = 10
		out := new(bytes.Buffer)
		for _, ts := range test.t {
			test.f.FormatTime(&s, ts, out)
			out.WriteString(" ")
			test.f.FormatLevel(&s, LevelInfo, out)
			out.WriteString(" ")
			test.f.FormatMessage(&s, "hello\nworld", false, out)
			out.WriteString(" ")
			test.f.FormatField(&s, "a", "field", out)
			out.WriteString(" ")
			test.f.FormatField(&s, "b", map[string]interface{}{"nesting": "is real"}, out)
			out.WriteString("\n")
		}
		if diff := cmp.Diff(out.String(), test.want); diff != "" {
			t.Errorf("output: %s", diff)
		}
	}

	// Test a panicing formatter.
	err := func() (err error) {
		defer func() {
			if x := recover(); x != nil {
				err = fmt.Errorf("recover: %v", x)
			}
		}()
		(&DefaultOutputFormatter{Aurora: aurora.NewAurora(true)}).FormatField(new(State), "fun", func() {}, new(bytes.Buffer))
		return
	}()
	if err == nil {
		t.Fatal("formatting a function should have caused a panic, but didn't")
	}

	// Test that that highlighting does something.
	var a, b = new(bytes.Buffer), new(bytes.Buffer)
	f := &DefaultOutputFormatter{Aurora: aurora.NewAurora(false)}
	f.FormatMessage(nil, "test", false, a)
	f.FormatMessage(nil, "test", true, b)
	if got, want := a.String(), b.String(); got != want {
		t.Errorf("message should be the same when uncolored aurora is in use:\n  got: %v\n want: %v", got, want)
	}
	f = &DefaultOutputFormatter{Aurora: aurora.NewAurora(true)}
	f.FormatMessage(nil, "test", false, a)
	f.FormatMessage(nil, "test", true, b)
	if got, want := a.String(), b.String(); got == want {
		t.Errorf("message should be different when colored aurora is in use:\n  got: %v\n want: %v", got, want)
	}
}

func TestLevelLength(t *testing.T) {
	for _, color := range []bool{false} {
		f := &DefaultOutputFormatter{Aurora: aurora.NewAurora(color)}
		var s State
		buf := new(bytes.Buffer)
		f.FormatLevel(&s, Level(0), buf)
		want := buf.Len()

		for i := LevelTrace; i < 100; i++ {
			var s State
			buf := new(bytes.Buffer)
			f.FormatLevel(&s, i, buf)
			if got := buf.Len(); got != want {
				t.Errorf("length of formmated level %v (color: %v):\n  got: %v\n want: %v", i, color, got, want)
			}
		}
	}
}
