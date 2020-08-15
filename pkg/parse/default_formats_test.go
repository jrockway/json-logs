package parse

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/logrusorgru/aurora/v3"
)

var defaultTime = time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)

func TestFormatting(t *testing.T) {
	programStartTime = defaultTime.Add(2*time.Hour + 3*time.Minute + 4*time.Second + 500*time.Millisecond + 600*time.Microsecond)
	testData := []struct {
		f    *DefaultOutputFormatter
		t    time.Time
		want string
	}{
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: false,
				AbsoluteTimeFormat:   time.RFC3339,
			},
			t:    defaultTime,
			want: `2000-01-02T03:04:05Z INFO  hello a:field b:{"nesting":"is real"}`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   time.RFC3339,
			},
			t:    defaultTime,
			want: `2000-01-02T03:04:05Z INFO  hello a:field b:↑`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   time.RFC3339,
			},
			t:    time.Time{},
			want: `       ??? INFO  hello a:field b:↑`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
			},
			t:    defaultTime,
			want: `-2h3m4s    INFO  hello a:field b:↑`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
			},
			t:    programStartTime.Add(-123),
			want: `-123ns     INFO  hello a:field b:↑`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
			},
			t:    programStartTime.Add(-123456),
			want: `-123µs     INFO  hello a:field b:↑`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
			},
			t:    programStartTime.Add(-123456789),
			want: `-123ms     INFO  hello a:field b:↑`,
		},
	}

	for _, test := range testData {
		var s State
		s.lastFields = map[string][]byte{"b": []byte(`{"nesting":"is real"}`)}
		s.timePadding = 10
		tz = time.UTC

		out := new(bytes.Buffer)
		if err := test.f.FormatTime(&s, test.t, out); err != nil {
			t.Errorf("time: %v", err)
		}
		out.WriteString(" ")
		if err := test.f.FormatLevel(&s, LevelInfo, out); err != nil {
			t.Errorf("level: %v", err)
		}
		out.WriteString(" ")
		if err := test.f.FormatMessage(&s, "hello", out); err != nil {
			t.Errorf("message: %v", err)
		}
		out.WriteString(" ")
		if err := test.f.FormatField(&s, "a", "field", out); err != nil {
			t.Errorf("a field: %v", err)
		}
		out.WriteString(" ")
		if err := test.f.FormatField(&s, "b", map[string]interface{}{"nesting": "is real"}, out); err != nil {
			t.Errorf("b field: %v", err)
		}
		if diff := cmp.Diff(out.String(), test.want); diff != "" {
			t.Errorf("output: %s", diff)
		}
	}

	if err := (&DefaultOutputFormatter{Aurora: aurora.NewAurora(true)}).FormatField(new(State), "fun", func() {}, new(bytes.Buffer)); err == nil {
		t.Error("formatting a function should fail, but didn't")
	}
}

func TestLevelLength(t *testing.T) {
	for _, color := range []bool{true, false} {
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
				fmt.Errorf("length of formmated level %v (color: %v):\n  got: %v\n want: %v", i, color, got, want)
			}
		}
	}
}
