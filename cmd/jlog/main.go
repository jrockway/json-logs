package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/itchyny/gojq"
	"github.com/jessevdk/go-flags"
	"github.com/jrockway/json-logs/pkg/parse"
	aurora "github.com/logrusorgru/aurora/v3"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

type output struct {
	NoElideDuplicates  bool   `long:"no-elide" description:"Disable eliding repeated fields.  By default, fields that have the same value as the line above them have their values replaced with '↑'."`
	RelativeTimestamps bool   `short:"r" long:"relative" description:"Print timestamps as a duration since the program started instead of absolute timestamps."`
	TimeFormat         string `short:"t" long:"time-format" description:"A go time.Format string describing how to format timestamps, or one of 'rfc3339', 'unix', 'stamp(milli|micro|nano)'." default:"stamp"`
	NoSummary          bool   `long:"no-summary" description:"Suppress printing the summary at the end."`
}
type general struct {
	JQ           string `short:"e" description:"A jq program to run on the processed input; use this to ignore certain lines, add fields, etc."`
	NoColor      bool   `short:"m" long:"no-color" description:"Disable the use of color."`
	NoMonochrome bool   `short:"c" long:"no-monochrome" description:"Force the use of color."`
}

type input struct {
	Lax          bool   `short:"l" long:"lax" description:"If true, suppress any validation errors including non-JSON log lines and missing timestamps, levels, and message.  We extract as many of those as we can, but if something is missing, the errors will be silently discarded."`
	LevelKey     string `long:"levelkey" default:"level" description:"JSON key that holds the log level."`
	TimestampKey string `long:"timekey" default:"ts" description:"JSON key that holds the log timestamp."`
	MessageKey   string `long:"messagekey" default:"msg" description:"JSON key that holds the log message."`
}

func main() {
	var gen general
	var in input
	var out output
	fp := flags.NewParser(nil, flags.HelpFlag|flags.PassDoubleDash)
	if _, err := fp.AddGroup("Input Schema", "", &in); err != nil {
		panic(err)
	}
	if _, err := fp.AddGroup("Output Format", "foo", &out); err != nil {
		panic(err)
	}
	if _, err := fp.AddGroup("General", "bar", &gen); err != nil {
		panic(err)
	}

	if _, err := fp.Parse(); err != nil {
		if ferr, ok := err.(*flags.Error); ok && ferr.Type == flags.ErrHelp {
			fmt.Fprintf(os.Stderr, "jlog - Search and pretty-print your JSON logs.\nMore info: https://github.com/jrockway/jlog\n")
			fmt.Fprintf(os.Stderr, ferr.Message)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "flag parsing: %v\n", err)
		os.Exit(3)
	}
	switch strings.ToLower(out.TimeFormat) {
	case "rfc3339":
		out.TimeFormat = time.RFC3339
	case "unix":
		out.TimeFormat = time.UnixDate
	case "stamp":
		out.TimeFormat = time.Stamp
	case "stampmilli":
		out.TimeFormat = time.StampMilli
	case "stampmicro":
		out.TimeFormat = time.StampMicro
	case "stampnano":
		out.TimeFormat = time.StampNano
	}
	if out.RelativeTimestamps {
		out.TimeFormat = ""
	}
	var jq *gojq.Code
	if p := gen.JQ; p != "" {
		q, err := gojq.Parse(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "problem parsing jq program %q:\n%v\n", p, err)
			os.Exit(1)
		}
		jq, err = gojq.Compile(q, gojq.WithVariables(parse.DefaultVariables))
		if err != nil {
			fmt.Fprintf(os.Stderr, "problem compiling jq program %q:\n%v\n", p, err)
			os.Exit(1)
		}
	}

	ins := &parse.InputSchema{
		LevelKey:    in.LevelKey,
		MessageKey:  in.MessageKey,
		TimeKey:     in.TimestampKey,
		TimeFormat:  parse.DefaultTimeParser,
		LevelFormat: parse.DefaultLevelParser,
		Strict:      !in.Lax,
	}

	var wantColor = isatty.IsTerminal(os.Stdout.Fd())
	switch {
	case gen.NoColor && gen.NoMonochrome:
		fmt.Fprintf(os.Stderr, "--no-color and --no-monochrome; if you're not sure, just let me decide!\n")
	case gen.NoColor:
		wantColor = false
	case gen.NoMonochrome:
		wantColor = true
	}

	outs := &parse.OutputSchema{
		Formatter: &parse.DefaultOutputFormatter{
			Aurora:               aurora.NewAurora(wantColor),
			ElideDuplicateFields: !out.NoElideDuplicates,
			AbsoluteTimeFormat:   out.TimeFormat,
		},
	}

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGPIPE)
	go func() {
		c := <-sigCh
		os.Stderr.Write([]byte("signal: " + c.String() + "\n"))
		os.Stdin.Close()
		signal.Stop(sigCh)
	}()

	summary, err := parse.ReadLog(os.Stdin, colorable.NewColorableStdout(), ins, outs, jq)
	if err != nil {
		outs.EmitError(err.Error())
	}

	if !out.NoSummary {
		lines := "1 line read"
		if n := summary.Lines; n != 1 {
			lines = fmt.Sprintf("%d lines read", n)
		}
		if n := summary.Filtered; n > 1 {
			lines += fmt.Sprintf(" (%d lines filtered)", n)
		} else if n == 1 {
			lines += " (1 line filtered)"
		}
		errors := "; no parse errors"
		if n := summary.Errors; n == 1 {
			errors = "; 1 parse error"
		} else if n > 1 {
			errors = fmt.Sprintf("; %d parse errors", n)
		}
		fmt.Fprintf(os.Stderr, "  %s%s.\n", lines, errors)
	}
	if errors.Is(err, syscall.EPIPE) {
		os.Exit(2)
	} else if err != nil {
		os.Exit(1)
	}
}
