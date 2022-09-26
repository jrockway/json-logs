package jlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jrockway/json-logs/pkg/parse"
	aurora "github.com/logrusorgru/aurora/v3"
	"github.com/mattn/go-isatty"
)

type Output struct {
	NoElideDuplicates  bool     `long:"no-elide" description:"Disable eliding repeated fields.  By default, fields that have the same value as the line above them have their values replaced with '↑'." env:"JLOG_NO_ELIDE_DUPLICATES"`
	RelativeTimestamps bool     `short:"r" long:"relative" description:"Print timestamps as a duration since the program started instead of absolute timestamps." env:"JLOG_RELATIVE_TIMESTAMPS"`
	TimeFormat         string   `short:"t" long:"time-format" description:"A go time.Format string describing how to format timestamps, or one of 'rfc3339(milli|micro|nano)', 'unix', 'stamp(milli|micro|nano)', or 'kitchen'." default:"stamp" env:"JLOG_TIME_FORMAT"`
	OnlySubseconds     bool     `short:"s" long:"only-subseconds" description:"Display only the fractional part of times that are in the same second as the last log line.  Only works with the (milli|micro|nano) formats above.  (This can be revisited, but it's complicated.)" env:"JLOG_ONLY_SUBSECONDS"`
	NoSummary          bool     `long:"no-summary" description:"Suppress printing the summary at the end." env:"JLOG_NO_SUMMARY"`
	PriorityFields     []string `long:"priority" short:"p" description:"A list of fields to show first; repeatable." env:"JLOG_PRIORITY_FIELDS" env-delim:","`
	HighlightFields    []string `long:"highlight" short:"H" description:"A list of fields to visually distinguish; repeatable." env:"JLOG_HIGHLIGHT_FIELDS" env-delim:"," default:"err" default:"error" default:"warn" default:"warning"` //nolint

	AfterContext  int `long:"after-context" short:"A" default:"0" description:"Print this many filtered lines after a non-filtered line (like grep)."`
	BeforeContext int `long:"before-context" short:"B" default:"0" description:"Print this many filtered lines before a non-filtered line (like grep)."`
	Context       int `long:"context" short:"C" default:"0" description:"Print this many context lines around each match (like grep)."`
}

type General struct {
	MatchRegex   string             `short:"g" long:"regex" description:"A regular expression that removes lines from the output that don't match, like grep."`
	NoMatchRegex string             `short:"G" long:"no-regex" description:"A regular expression that removes lines from the output that DO match, like 'grep -v'."`
	RegexpScope  *parse.RegexpScope `short:"S" long:"regex-scope" description:"Where to apply the provided regex; (m)essage, (k)eys, or (v)alues. 'kmv' looks in all scopes, 'k' only searches keys, etc." default:"kmv"`
	JQ           string             `short:"e" long:"jq" description:"A jq program to run on each record in the processed input; use this to ignore certain lines, add fields, etc.  Hint: 'select(condition)' will remove lines that don't match 'condition'."`
	JQSearchPath []string           `long:"jq-search-path" env:"JLOG_JQ_SEARCH_PATH" description:"A list of directories in which to search for JQ modules.  A path entry named (not merely ending in) .jq is automatically loaded.  When set through the environment, use ':' as the delimiter (like $PATH)." default:"~/.jq" default:"~/.jlog/jq/.jq" default:"~/.jlog/jq" env-delim:":"` //nolint
	NoColor      bool               `short:"M" long:"no-color" description:"Disable the use of color." env:"JLOG_FORCE_MONOCHROME"`
	NoMonochrome bool               `short:"c" long:"no-monochrome" description:"Force the use of color." env:"JLOG_FORCE_COLOR"`
	Profile      string             `long:"profile" description:"If set, collect a CPU profile and write it to this file."`

	Version bool `short:"v" long:"version" description:"Print version information and exit."`
}

type Input struct {
	Lax            bool     `short:"l" long:"lax" description:"If true, suppress any validation errors including non-JSON log lines and missing timestamps, levels, and message.  We extract as many of those as we can, but if something is missing, the errors will be silently discarded." env:"JLOG_LAX"`
	LevelKey       string   `long:"levelkey" description:"JSON key that holds the log level." env:"JLOG_LEVEL_KEY"`
	NoLevelKey     bool     `long:"nolevelkey" description:"If set, don't look for a log level, and don't display levels." env:"JLOG_NO_LEVEL_KEY"`
	TimestampKey   string   `long:"timekey" description:"JSON key that holds the log timestamp." env:"JLOG_TIMESTAMP_KEY"`
	NoTimestampKey bool     `long:"notimekey" description:"If set, don't look for a time, and don't display times." env:"JLOG_NO_TIMESTAMP_KEY"`
	MessageKey     string   `long:"messagekey" description:"JSON key that holds the log message." env:"JLOG_MESSAGE_KEY"`
	NoMessageKey   bool     `long:"nomessagekey" description:"If set, don't look for a message, and don't display messages (time/level + fields only)." env:"JLOG_NO_MESSAGE_KEY"`
	DeleteKeys     []string `long:"delete" description:"JSON keys to be deleted before JQ processing and output; repeatable." env:"JLOG_DELETE_KEYS" env-delim:","`
	UpgradeKeys    []string `long:"upgrade" description:"JSON key (of type object) whose fields should be merged with any other fields; good for loggers that always put structed data in a separate key; repeatable.\n--upgrade b would transform as follows: {a:'a', b:{'c':'c'}} -> {a:'a', c:'c'}" env:"JLOG_UPGRADE_KEYS" env-delim:","`
}

func NewInputSchema(in Input) (*parse.InputSchema, error) { //nolint
	ins := &parse.InputSchema{
		Strict: !in.Lax,
	}
	if in.NoLevelKey {
		ins.LevelKey = ""
		ins.LevelFormat = parse.NoopLevelParser
		ins.NoLevelKey = true
	} else if k := in.LevelKey; k != "" {
		ins.LevelKey = k
		ins.LevelFormat = parse.DefaultLevelParser
	}
	if in.NoMessageKey {
		ins.MessageKey = ""
		ins.NoMessageKey = true
	} else if k := in.MessageKey; k != "" {
		ins.MessageKey = k
	}
	if in.NoTimestampKey {
		ins.TimeKey = ""
		ins.TimeFormat = parse.NoopTimeParser
		ins.NoTimeKey = true
	} else if k := in.TimestampKey; k != "" {
		ins.TimeKey = k
		ins.TimeFormat = parse.DefaultTimeParser
	}
	if u := in.UpgradeKeys; len(u) > 0 {
		ins.UpgradeKeys = append(ins.UpgradeKeys, u...)
	}
	return ins, nil
}

func NewOutputFormatter(out Output, gen General) (*parse.OutputSchema, error) { //nolint
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

	var wantColor = isatty.IsTerminal(os.Stdout.Fd())
	switch {
	case gen.NoColor && gen.NoMonochrome:
		fmt.Fprintf(os.Stderr, "--no-color and --no-monochrome; if you're not sure, just let me decide!\n")
	case gen.NoColor:
		wantColor = false
	case gen.NoMonochrome:
		wantColor = true
	}

	defaultOutput := &parse.DefaultOutputFormatter{
		Aurora:               aurora.NewAurora(wantColor),
		ElideDuplicateFields: !out.NoElideDuplicates,
		AbsoluteTimeFormat:   out.TimeFormat,
		SubSecondsOnlyFormat: subsecondFormt,
		Zone:                 time.Local,
		HighlightFields:      make(map[string]struct{}),
	}
	for _, k := range out.HighlightFields {
		defaultOutput.HighlightFields[k] = struct{}{}
	}

	outs := &parse.OutputSchema{
		Formatter:      defaultOutput,
		PriorityFields: out.PriorityFields,
		AfterContext:   out.Context,
		BeforeContext:  out.Context,
	}

	// Let -A and -B override -C.
	if a := out.AfterContext; a > 0 {
		outs.AfterContext = a
	}
	if b := out.BeforeContext; b > 0 {
		outs.BeforeContext = b
	}

	return outs, nil
}

func NewFilterScheme(gen General) (*parse.FilterScheme, error) { //nolint
	fsch := new(parse.FilterScheme)
	if gen.MatchRegex != "" && gen.NoMatchRegex != "" {
		return nil, errors.New("cannot have both a non-empty MatchRegex and a non-empty NoMatchRegex")
	}
	if err := fsch.AddMatchRegex(gen.MatchRegex); err != nil {
		return nil, fmt.Errorf("adding MatchRegex: %v", err)
	}
	if err := fsch.AddNoMatchRegex(gen.NoMatchRegex); err != nil {
		return nil, fmt.Errorf("adding NoMatchRegex: %v", err)
	}
	if err := fsch.AddJQ(gen.JQ, &parse.JQOptions{SearchPath: gen.JQSearchPath}); err != nil {
		return nil, fmt.Errorf("adding JQ: %v", err)
	}
	if gen.RegexpScope != nil {
		fsch.Scope = *gen.RegexpScope
	}
	return fsch, nil
}

func PrintOutputSummary(out Output, summary parse.Summary, w io.Writer) { //nolint
	if out.NoSummary {
		return
	}
	fmt.Fprintf(w, "  "+summary.String()+"\n")
}
