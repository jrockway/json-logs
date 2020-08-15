package parse

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/logrusorgru/aurora/v3"
)

func TestFormatting(t *testing.T) {
	testData := []struct {
		f    *DefaultOutputFormatter
		want string
	}{
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: false,
				AbsoluteTimeFormat:   time.RFC3339,
			},
			want: `2000-01-01T22:04:05-05:00  INFO  hello a:field b:{"nesting":"is real"}`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   time.RFC3339,
			},
			want: `2000-01-01T22:04:05-05:00  INFO  hello a:field b:↑`,
		},
		{
			f: &DefaultOutputFormatter{
				Aurora:               aurora.NewAurora(false),
				ElideDuplicateFields: true,
				AbsoluteTimeFormat:   "",
			},
			want: `-2h3m4s                    INFO  hello a:field b:↑`,
		},
	}

	for _, test := range testData {
		var s State
		logTime := time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
		programStartTime = logTime.Add(2*time.Hour + 3*time.Minute + 4*time.Second + 500*time.Millisecond + 600*time.Microsecond)
		s.lastFields = map[string][]byte{"b": []byte(`{"nesting":"is real"}`)}
		s.timePadding = 26
		out := new(bytes.Buffer)
		if err := test.f.FormatTime(&s, logTime, out); err != nil {
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
}
