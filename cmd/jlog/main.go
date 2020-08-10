package main

import (
	"fmt"
	"os"
	"time"

	"github.com/itchyny/gojq"
	"github.com/jessevdk/go-flags"
	"github.com/jrockway/json-logs/pkg/parse"
	aurora "github.com/logrusorgru/aurora/v3"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

type general struct {
	NoColor            bool     `long:"nocolor" description:"Disable the use of color."`
	NoElideDuplicates  bool     `long:"noelide" description:"Disable eliding repeated fields."`
	RelativeTimestamps bool     `long:"relative" short:"r" description:"Print timestamps as a duration since the program started instead of absolute timestamps."`
	AbsoluteEvery      int      `long:"absolute-every" description:"A compromise between relative and absolute timestamps; output an absolute timestamp, and then a duration relative to that timestamp until --absolute-every lines have been printed.  Implies --relative-timestamps." default:"0"`
	TimeFormat         string   `long:"time-format" short:"t" description:"A go time.Format string describing how to format timestamps; or 'RFC3339'." default:"RFC3339"`
	Lax                bool     `long:"lax" description:"If true, suppress any validation errors including non-JSON log lines and missing timestamps, levels, and message.  We extract as many of those as we can, but if something is missing, the errors will be silently discarded."`
	NoSummary          bool     `long:"no-summary" description:"Suppress printing the summary at the end."`
	JQPrograms         []string `short:"e" description:"Zero or more JQ programs to run on the processed input; use this to ignore certain lines, add fields, etc.  Repeatable; output from previous stages is fed into the next stage.  \"jlog -e 'foo | bar'\" is equivalent to \"jlog -e foo -e bar\"."`
}

type inputFormat struct {
	LevelKey     string `long:"levelkey" default:"level" description:"JSON key that holds the log level."`
	TimestampKey string `long:"timekey" default:"ts" description:"JSON key that holds the log timestamp."`
	MessageKey   string `long:"messagekey" default:"msg" description:"JSON key that holds the log message."`
}

func main() {
	var gen general
	var inf inputFormat
	fp := flags.NewParser(nil, flags.HelpFlag|flags.PassDoubleDash)
	if _, err := fp.AddGroup("General", "", &gen); err != nil {
		panic(err)
	}
	if _, err := fp.AddGroup("Input Schema", "", &inf); err != nil {
		panic(err)
	}
	if _, err := fp.Parse(); err != nil {
		if ferr, ok := err.(*flags.Error); ok && ferr.Type == flags.ErrHelp {
			fmt.Fprintf(os.Stderr, ferr.Message)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "flag parsing: %v\n", err)
		os.Exit(3)
	}
	if gen.TimeFormat == "RFC3339" {
		gen.TimeFormat = time.RFC3339
	}
	if gen.RelativeTimestamps {
		gen.TimeFormat = ""
	}
	var programs []*gojq.Code
	for _, p := range gen.JQPrograms {
		q, err := gojq.Parse(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "problem parsing jq program %q:\n%v\n", p, err)
			os.Exit(1)
		}
		c, err := gojq.Compile(q, gojq.WithVariables([]string{"$NOW", "$TS", "$MSG", "$LVL"}))
		if err != nil {
			fmt.Fprintf(os.Stderr, "problem compiling jq program %q:\n%v\n", p, err)
			os.Exit(1)
		}
		programs = append(programs, c)
	}
	ins := &parse.InputSchema{
		LevelKey:    inf.LevelKey,
		MessageKey:  inf.MessageKey,
		TimeKey:     inf.TimestampKey,
		TimeFormat:  parse.DefaultTimeParser,
		LevelFormat: parse.DefaultLevelParser,
		Strict:      !gen.Lax,
	}
	wantColor := isatty.IsTerminal(os.Stdout.Fd()) && !gen.NoColor
	outs := &parse.OutputSchema{
		Formatter: &parse.DefaultOutputFormatter{
			Aurora:               aurora.NewAurora(wantColor),
			TimePrecision:        time.Second,
			ElideDuplicateFields: !gen.NoElideDuplicates,
			AbsoluteTimeFormat:   gen.TimeFormat,
			AbsoluteEvery:        gen.AbsoluteEvery,
		},
	}
	summary, err := parse.ReadLog(os.Stdin, colorable.NewColorableStdout(), ins, outs, programs)
	if err != nil {
		outs.EmitError(err.Error())
	}
	os.Stdout.Close()
	if !gen.NoSummary {
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
}
