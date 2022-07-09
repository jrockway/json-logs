package jlog

import (
	"strings"
	"testing"

	"github.com/jessevdk/go-flags"
	"github.com/jrockway/json-logs/pkg/parse"
)

func TestEmpty(t *testing.T) {
	if _, err := NewInputSchema(Input{}); err != nil {
		t.Errorf("new input schema: %v", err)
	}
	if _, err := NewOutputFormatter(Output{}, General{}); err != nil {
		t.Errorf("new output schema: %v", err)
	}
	if _, err := NewFilterScheme(General{}); err != nil {
		t.Errorf("new filter scheme: %v", err)
	}
}

func TestFlagParsing(t *testing.T) {
	testData := []struct {
		name  string
		flags []string
	}{
		{
			name: "default",
		},
		{
			name: "short",
			flags: []string{
				"-r", "-t", "rfc3339nano", "-s", "-p", "foo", "-H", "bar",
				"-A", "1", "-B", "2", "-C", "3",
				"-g", ".",
				"-S", "k",
				"-e", "select(true)",
				"-M", "-c",
				"-l",
			},
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var gen General
			var in Input
			var out Output
			fp := flags.NewParser(nil, flags.HelpFlag)
			if _, err := fp.AddGroup("Input Schema", "", &in); err != nil {
				t.Errorf("add group: %v", err)
			}
			if _, err := fp.AddGroup("Output Format", "foo", &out); err != nil {
				t.Errorf("add group: %v", err)
			}
			if _, err := fp.AddGroup("General", "bar", &gen); err != nil {
				t.Errorf("add group: %v", err)
			}
			if _, err := fp.ParseArgs(test.flags); err != nil {
				t.Errorf("parse args: %v", err)
			}
			if _, err := NewInputSchema(in); err != nil {
				t.Errorf("new input schema: %v", err)
			}
			if _, err := NewOutputFormatter(out, gen); err != nil {
				t.Errorf("new output schema: %v", err)
			}
			if _, err := NewFilterScheme(gen); err != nil {
				t.Errorf("new filter scheme: %v", err)
			}
		})
	}
}

func TestPrintOutputSummary(t *testing.T) {
	w := new(strings.Builder)
	PrintOutputSummary(Output{}, parse.Summary{}, w)
	PrintOutputSummary(Output{NoSummary: true}, parse.Summary{}, w)
	if got, want := w.String(), "  0 lines read; no parse errors.\n"; got != want {
		t.Errorf("output:\n  got: %q\n want: %q", got, want)
	}
}
