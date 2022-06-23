package jlog

import (
	"strings"
	"testing"

	"github.com/jrockway/json-logs/pkg/parse"
)

func TestDefaults(t *testing.T) {
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

func TestPrintOutputSummary(t *testing.T) {
	w := new(strings.Builder)
	PrintOutputSummary(Output{}, parse.Summary{}, w)
	PrintOutputSummary(Output{NoSummary: true}, parse.Summary{}, w)
	if got, want := w.String(), "  0 lines read; no parse errors.\n"; got != want {
		t.Errorf("output:\n  got: %q\n want: %q", got, want)
	}
}
