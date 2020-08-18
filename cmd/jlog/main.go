package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/jessevdk/go-flags"
	"github.com/jrockway/json-logs/pkg/parse"
	aurora "github.com/logrusorgru/aurora/v3"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

type output struct {
	NoElideDuplicates  bool     `long:"no-elide" description:"Disable eliding repeated fields.  By default, fields that have the same value as the line above them have their values replaced with '↑'." env:"JLOG_NO_ELIDE_DUPLICATES"`
	RelativeTimestamps bool     `short:"r" long:"relative" description:"Print timestamps as a duration since the program started instead of absolute timestamps." env:"JLOG_RELATIVE_TIMESTAMPS"`
	TimeFormat         string   `short:"t" long:"time-format" description:"A go time.Format string describing how to format timestamps, or one of 'rfc3339(milli|micro|nano)', 'unix', 'stamp(milli|micro|nano)', or 'kitchen'." default:"stamp" env:"JLOG_TIME_FORMAT"`
	OnlySubseconds     bool     `short:"s" long:"only-subseconds" description:"Display only the fractional part of times that are in the same second as the last log line.  Only works with the (milli|micro|nano) formats above.  (This can be revisited, but it's complicated.)" env:"JLOG_ONLY_SUBSECONDS"`
	NoSummary          bool     `long:"no-summary" description:"Suppress printing the summary at the end." env:"JLOG_NO_SUMMARY"`
	PriorityFields     []string `long:"priority" short:"p" description:"A list of fields to show first; repeatable." env:"JLOG_PRIORITY_FIELDS" env-delim:","`
}
type general struct {
	JQ           string `short:"e" description:"A jq program to run on the processed input; use this to ignore certain lines, add fields, etc."`
	NoColor      bool   `short:"M" long:"no-color" description:"Disable the use of color." env:"JLOG_FORCE_MONOCHROME"`
	NoMonochrome bool   `short:"C" long:"no-monochrome" description:"Force the use of color." ENV:"JLOG_FORCE_COLOR"`
	Profile      string `long:"profile" description:"If set, collect a CPU profile and write it to this file."`
}

type input struct {
	Lax          bool   `short:"l" long:"lax" description:"If true, suppress any validation errors including non-JSON log lines and missing timestamps, levels, and message.  We extract as many of those as we can, but if something is missing, the errors will be silently discarded." env:"JLOG_LAX"`
	LevelKey     string `long:"levelkey" description:"JSON key that holds the log level." env:"JLOG_LEVEL_KEY"`
	TimestampKey string `long:"timekey" description:"JSON key that holds the log timestamp." env:"JLOG_TIMESTAMP_KEY"`
	MessageKey   string `long:"messagekey" description:"JSON key that holds the log message." env:"JLOG_MESSAGE_KEY"`
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
			fmt.Fprintf(os.Stderr, "jlog - Search and pretty-print your JSON logs.\nMore info: https://github.com/jrockway/json-logs\n")
			fmt.Fprintf(os.Stderr, ferr.Message)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "flag parsing: %v\n", err)
		os.Exit(3)
	}
	var f *os.File
	if gen.Profile != "" {
		var err error
		f, err = os.Create(gen.Profile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
	}
	// This has a terrible variable name so that =s align below.
	var subsecondFormt string
	switch strings.ToLower(out.TimeFormat) {
	case "rfc3339":
		out.TimeFormat = time.RFC3339
	case "rfc3339milli":
		out.TimeFormat = "2006-01-02T15:04:05.000Z07:00"
		subsecondFormt = "                   .000"
	case "rfc3339micro":
		out.TimeFormat = "2006-01-02T15:04:05.000000Z07:00"
		subsecondFormt = "                   .000000"
	case "rfc3339nano":
		// time.RFC3339Nano is pretty ugly to look at, because it removes any zeros at the
		// end of the seconds field.  This adds them back in, so times are always the same
		// length.
		out.TimeFormat = "2006-01-02T15:04:05.000000000Z07:00"
		subsecondFormt = "                   .000000000"
	case "unix":
		out.TimeFormat = time.UnixDate
	case "stamp":
		//               "Jan _2 15:04:05"
		out.TimeFormat = time.Stamp
	case "stampmilli":
		//               "Jan _2 15:04:05.000"
		out.TimeFormat = time.StampMilli
		subsecondFormt = "               .000"
	case "stampmicro":
		//               "Jan _2 15:04:05.000000"
		out.TimeFormat = time.StampMicro
		subsecondFormt = "               .000000"
	case "stampnano":
		//               "Jan _2 15:04:05.000000000"
		out.TimeFormat = time.StampNano
		subsecondFormt = "               .000000000"
	case "kitchen":
		out.TimeFormat = time.Kitchen
	}
	if out.RelativeTimestamps {
		out.TimeFormat = ""
	}
	if !out.OnlySubseconds {
		subsecondFormt = ""
	}

	jq, err := parse.CompileJQ(gen.JQ)
	if err != nil {
		fmt.Fprintf(os.Stderr, "problem %v\n", err)
		os.Exit(1)
	}

	ins := &parse.InputSchema{
		Strict: !in.Lax,
	}
	if k := in.LevelKey; k != "" {
		ins.LevelKey = k
		ins.LevelFormat = parse.DefaultLevelParser
	}
	if k := in.MessageKey; k != "" {
		ins.MessageKey = k
	}
	if k := in.TimestampKey; k != "" {
		ins.TimeKey = k
		ins.TimeFormat = parse.DefaultTimeParser
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
			SubSecondsOnlyFormat: subsecondFormt,
			Zone:                 time.Local,
		},
		PriorityFields: out.PriorityFields,
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
	if f != nil {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write CPU profile: %v\n", err)
		}
	}
	if errors.Is(err, syscall.EPIPE) {
		os.Exit(2)
	} else if err != nil {
		os.Exit(1)
	}
}
