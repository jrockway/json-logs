package parse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/logrusorgru/aurora/v3"
)

// TimeParser is a function that parses timestamps in log messages.
type TimeParser func(interface{}) (time.Time, error)

// LevelParser is a function that parses log levels in log messages.
type LevelParser func(interface{}) (Level, error)

// Level is a log level.  This exists so that you can write jq expressions like
// "select($LVL<$WARN)".  Whatever logger you're using probably has totally different levels because
// nobody can agree on them.  Feel free to add them here in the right place.
type Level uint8

const (
	LevelUnknown Level = iota
	LevelTrace
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelPanic
	LevelDPanic
	LevelFatal
)

// LineBufferSize is the longest we're willing to look for a newline in the input.
const LineBufferSize = 1 * 1024 * 1024 // 1 MiB

// InputSchema controls the interpretation of incoming log lines.
type InputSchema struct {
	TimeKey     string      // The name of the key that holds the timestamp.
	TimeFormat  TimeParser  // How to turn the value of the time key into a time.Time.
	LevelKey    string      // The name of the key that holds the log level.
	LevelFormat LevelParser // How to turn the value of the level key into a Level.
	MessageKey  string      // The name of the key that holds the main log message.

	NoTimeKey    bool // If set, suppress any time handling.
	NoLevelKey   bool // If set, suppress any level handling.
	NoMessageKey bool // If set, suppress any message handling.

	// If true, print an error when non-JSON lines appear in the input.  If false, treat them
	// as normal messages with as much information extracted as possible.
	Strict bool

	// DeleteKeys is a list of keys to delete; used when the log lines contain version
	// information that is used for guessing the schema.
	DeleteKeys []string

	// UpgradeKeys is a list of keys to merge into the raw data.  For example, lager puts
	// everything in the "data" key.
	UpgradeKeys []string
}

// OutputFormatter describes an object that actually does the output formatting.  Methods take a
// bytes.Buffer so they can output incrementally as with an io.Writer, but without worrying about
// write errors or short writes.
type OutputFormatter interface {
	// FormatTime is a function that formats a time.Time and outputs it to an io.Writer.
	FormatTime(s *State, t time.Time, w *bytes.Buffer)

	// FormatLevel is a function that formats a log level and outputs it to an io.Writer.
	FormatLevel(s *State, lvl Level, w *bytes.Buffer)

	// FormatMessage is a function that formats a log message and outputs it to an io.Writer.
	FormatMessage(s *State, msg string, highlight bool, w *bytes.Buffer)

	// FormatField is a function that formats a (key, value) pair and outputs it to an io.Writer.
	FormatField(s *State, k string, v interface{}, w *bytes.Buffer)
}

// State keeps state between log lines.
type State struct {
	// seenFields maintains an ordering of all fields, so that they are consistent between log
	// lines.
	seenFields []string
	// timePadding is the width of the time field, so that the next field in the output lines up
	// with the line above it.
	timePadding int
	// lastFields is the value of each field that was most recently seen.
	lastFields map[string][]byte
	// lastTime is the time of the last log line.
	lastTime time.Time
}

// OutputSchema controls how output lines are formatted.
type OutputSchema struct {
	PriorityFields []string         // PriorityFields controls which fields are printed first.
	Formatter      OutputFormatter  // Actually does the formatting.
	EmitErrorFn    func(msg string) // A function that sees all errors.
	BeforeContext  int              // Context lines to print before a match.
	AfterContext   int              // Context lines to print after a match.

	suppressionConfigured, noTime, noLevel, noMessage bool
	state                                             State // state carries context between lines
}

// EmitError prints any internal errors, so that log lines are not silently ignored if they are
// unparseable.
func (s *OutputSchema) EmitError(msg string) {
	if s.EmitErrorFn == nil {
		os.Stderr.WriteString("  ↳ " + msg + "\n")
	} else {
		s.EmitErrorFn(msg)
	}
}

// line represents one log line.
type line struct {
	time        time.Time
	msg         string
	lvl         Level
	raw         []byte
	highlight   bool
	fields      map[string]interface{}
	isSeparator bool // If true, this is not a line but a separator from context.
}

func (l *line) reset() {
	l.raw = nil
	l.msg = ""
	l.fields = make(map[string]interface{})
	l.lvl = LevelUnknown
	l.time = time.Time{}
	l.highlight = false
}

type Summary struct {
	Lines    int
	Errors   int
	Filtered int
}

func (s Summary) String() string {
	lines := "1 line read"
	if n := s.Lines; n != 1 {
		lines = fmt.Sprintf("%d lines read", n)
	}
	if n := s.Filtered; n > 1 {
		lines += fmt.Sprintf(" (%d lines filtered)", n)
	} else if n == 1 {
		lines += " (1 line filtered)"
	}
	errmsg := "; no parse errors"
	if n := s.Errors; n == 1 {
		errmsg = "; 1 parse error"
	} else if n > 1 {
		errmsg = fmt.Sprintf("; %d parse errors", n)
	}
	return fmt.Sprintf("%s%s.", lines, errmsg)
}

// ReadLog reads a stream of JSON-formatted log lines from the provided reader according to the
// input schema, reformatting it and writing to the provided writer according to the output schema.
// Parse errors are handled according to the input schema.  Any other errors, not including io.EOF
// on the reader, are returned.
func ReadLog(r io.Reader, w io.Writer, ins *InputSchema, outs *OutputSchema, filter *FilterScheme) (Summary, error) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, LineBufferSize), LineBufferSize)
	var l line
	outs.state = State{
		lastFields: make(map[string][]byte),
	}
	if outs.Formatter == nil {
		outs.Formatter = &DefaultOutputFormatter{
			Aurora: aurora.NewAurora(false),
		}
	}
	var sum Summary

	buf := new(bytes.Buffer)
	ctx := &context{
		After:  outs.AfterContext,
		Before: outs.BeforeContext,
	}

	for s.Scan() {
		sum.Lines++

		err := func() (retErr error) {
			var addError, writeRawLine, recoverable bool

			// Adjust counters, print debugging information, flush buffers on the way
			// out, no matter what.
			defer func() {
				if addError {
					sum.Errors++
				}
				var writeError bool
				if buf.Len() > 0 {
					if _, err := buf.WriteTo(w); err != nil {
						recoverable = false
						writeError = true
						if retErr != nil {
							retErr = fmt.Errorf("write remaining buffer content: %w (while flushing buffer after error %v)", err, retErr)
						} else {
							retErr = fmt.Errorf("write remaining buffer content: %w", err)
						}
					}
				}
				if writeRawLine {
					buf.Write(l.raw)
					buf.WriteString("\n")
					if _, err := buf.WriteTo(w); err != nil {
						writeError = true
						recoverable = false
						retErr = fmt.Errorf("write raw line: %w (while printing raw log that caused error %v)", err, retErr)
					}
				}
				if recoverable {
					if ins.Strict {
						outs.EmitError(retErr.Error())
					}
					retErr = nil
				}
				if writeError && !addError {
					sum.Errors++
				}
			}()

			// Scope panics to the line that caused them.
			defer func() {
				if err := recover(); err != nil {
					addError = true
					writeRawLine = true
					recoverable = false
					stack := make([]byte, 2048)
					runtime.Stack(stack, false)
					retErr = fmt.Errorf("%s\n%s", err, stack)
				}
			}()

			// Reset state from the last line.
			buf.Reset()
			l.reset()
			l.raw = s.Bytes()

			// Parse input.
			parseErr := ins.ReadLine(&l)

			// Show parse errors in strict mode.
			if parseErr != nil && ins.Strict {
				addError = true
				writeRawLine = true
				recoverable = true
				return fmt.Errorf("parse: %w", parseErr)
			}

			// Filter.
			filtered, err := filter.Run(&l)
			if err != nil {
				addError = true
				writeRawLine = true
				recoverable = false
				// It is questionable as to whether or not a filter breaking means
				// that we should stop processing the log entirely.  It's probably a
				// bug in the filter that affects every line, so the sooner we
				// return the error, the sooner the user can fix their filter.  But
				// on the other hand, is it worth it to spend the time debugging a
				// jq program that's only broken on one line out of a billion?
				return fmt.Errorf("filter: %w", err)
			}
			if filtered {
				sum.Filtered++
				if parseErr != nil {
					addError = true
					recoverable = true
					writeRawLine = false
					return fmt.Errorf("parse: %w", parseErr)
				}
			}

			// Emit any lines that are able to be printed based on the context settings.
			for _, toEmit := range ctx.Print(&l, !filtered) {
				if !outs.suppressionConfigured {
					outs.noTime = ins.NoTimeKey
					outs.noLevel = ins.NoLevelKey
					outs.noMessage = ins.NoMessageKey
					outs.suppressionConfigured = true
				}
				outs.Emit(toEmit, buf)
			}

			// Copying the buffer to the output writer is handled in defer.
			if parseErr != nil {
				addError = true
				writeRawLine = false
				recoverable = true
				return fmt.Errorf("parse: %w", err)
			}
			return nil
		}()
		if err != nil {
			return sum, fmt.Errorf("input line %d: %w", sum.Lines, err)
		}
	}
	return sum, s.Err()
}

// guessSchema tries to guess the schema if one has not been explicitly configured.
func (s *InputSchema) guessSchema(l *line) {
	if s.TimeKey != "" || s.LevelKey != "" || s.MessageKey != "" {
		// Explicitly turn off guessing, as per the docs.
		return
	}
	if s.NoTimeKey || s.NoLevelKey || s.NoMessageKey {
		// We can guess the schema in the presence of these options, but we currently don't
		// have any such schemas.
		return
	}
	has := func(key string) bool {
		_, ok := l.fields[key]
		return ok
	}
	if has("ts") && has("level") && has("msg") {
		// zap's default production encoder
		s.TimeKey = "ts"
		s.TimeFormat = StrictUnixTimeParser
		s.LevelKey = "level"
		s.LevelFormat = DefaultLevelParser
		s.MessageKey = "msg"
		return
	}
	if has("timestamp") && has("severity") && has("message") {
		// stackdriver
		s.TimeKey = "timestamp"
		s.TimeFormat = DefaultTimeParser
		s.LevelKey = "severity"
		s.LevelFormat = DefaultLevelParser
		s.MessageKey = "message"
		return
	}
	if has("time") && has("severity") && has("message") {
		// another stackdriver format
		s.TimeKey = "time"
		s.TimeFormat = DefaultTimeParser
		s.LevelKey = "severity"
		s.LevelFormat = DefaultLevelParser
		s.MessageKey = "message"
		return
	}
	if has("time") && has("level") && has("v") && has("msg") {
		// bunyan
		if v, ok := l.fields["v"].(float64); ok && v == 0 {
			s.TimeKey = "time"
			s.TimeFormat = DefaultTimeParser // RFC3339
			s.LevelKey = "level"
			s.LevelFormat = BunyanV0LevelParser
			s.MessageKey = "msg"
			s.DeleteKeys = append(s.DeleteKeys, "v")
			return
		}
	}
	if has("time") && has("level") && has("msg") {
		// logrus default json encoder
		s.TimeKey = "time"
		s.TimeFormat = DefaultTimeParser
		s.LevelKey = "level"
		s.LevelFormat = DefaultLevelParser
		s.MessageKey = "msg"
		return
	}
	if len(l.fields) == 5 && has("timestamp") && has("level") && has("message") && has("data") && has("source") {
		// lager "pretty"
		s.TimeKey = "timestamp"
		s.TimeFormat = DefaultTimeParser
		s.LevelKey = "level"
		s.LevelFormat = DefaultLevelParser
		s.MessageKey = "message"
		s.UpgradeKeys = append(s.UpgradeKeys, "data")
		return
	}
	if len(l.fields) == 5 && has("timestamp") && has("log_level") && has("message") && has("data") && has("source") {
		// lager non-pretty
		s.TimeKey = "timestamp"
		s.TimeFormat = StrictUnixTimeParser
		s.LevelKey = "log_level"
		s.LevelFormat = LagerLevelParser
		s.MessageKey = "message"
		s.UpgradeKeys = append(s.UpgradeKeys, "data")
		return
	}
	if has("ts") && has("message") && has("workerId") && has("pipelineName") {
		// Pachyderm worker logs.
		s.TimeKey = "ts"
		s.TimeFormat = DefaultTimeParser // RFC3339Nano
		s.NoLevelKey = true
		s.MessageKey = "message"
		return
	}
}

// ReadLine parses a log line into the provided line object.
func (s *InputSchema) ReadLine(l *line) error {
	var retErr error
	pushError := func(err error) {
		if retErr == nil {
			retErr = err
			return
		}
		retErr = fmt.Errorf("%v; %v", retErr, err)
	}

	if !s.Strict && ((len(l.raw) > 0 && l.raw[0] != '{') || len(l.raw) == 0) {
		l.time = time.Time{}
		l.msg = string(l.raw)
		return errors.New("not a JSON object")
	}
	if err := json.Unmarshal(l.raw, &l.fields); err != nil {
		pushError(fmt.Errorf("unmarshal json: %w", err))
		if !s.Strict {
			l.msg = string(l.raw)
		}
	}
	s.guessSchema(l)
	if !s.NoTimeKey {
		if raw, ok := l.fields[s.TimeKey]; s.TimeFormat != nil && ok {
			t, err := s.TimeFormat(raw)
			if err != nil {
				pushError(fmt.Errorf("parse time %T(%v) in key %q: %w", raw, raw, s.TimeKey, err))
			} else {
				delete(l.fields, s.TimeKey)
				l.time = t
			}
		} else {
			pushError(fmt.Errorf("no time key %q in incoming log", s.TimeKey))
		}
	}
	if !s.NoMessageKey {
		if msg, ok := l.fields[s.MessageKey]; ok {
			switch x := msg.(type) {
			case string:
				l.msg = x
				delete(l.fields, s.MessageKey)
			default:
				l.msg = string(l.raw)
				pushError(fmt.Errorf("message key %q contains non-string data (%q of type %T)", s.MessageKey, msg, msg))
			}
		} else {
			pushError(fmt.Errorf("no message key %q in incoming log", s.MessageKey))
		}
	}
	if !s.NoLevelKey {
		if lvl, ok := l.fields[s.LevelKey]; s.LevelFormat != nil && ok {
			if parsed, err := s.LevelFormat(lvl); err != nil {
				pushError(fmt.Errorf("level key %q: %w", s.LevelKey, err))
			} else {
				l.lvl = parsed
				delete(l.fields, s.LevelKey)
			}
		} else {
			pushError(fmt.Errorf("no level key %q in incoming log", s.LevelKey))
		}
	}
	for _, name := range s.UpgradeKeys {
		raw, ok := l.fields[name]
		if !ok {
			// Skip upgrade if the key is absent.
			continue
		}
		toMerge, ok := raw.(map[string]interface{})
		if ok {
			// Delete original first, so that foo:{foo:42} will overwrite to foo:42,
			// rather than {}.
			delete(l.fields, name)
			for k, v := range toMerge {
				l.fields[k] = v
			}
		} else if s.Strict {
			pushError(fmt.Errorf("upgrade key %q: invalid data type: want map[string]interface{}, got %T", name, raw))
		}
	}
	for _, k := range s.DeleteKeys {
		delete(l.fields, k)
	}
	return retErr
}

// Emit emits a formatted line to the provided buffer.  Emit must not mutate line.
func (s *OutputSchema) Emit(l *line, w *bytes.Buffer) {
	// Is this a line separating unrelated contexts?  If so, print a separator and do nothing else.
	if l.isSeparator {
		w.WriteString("---\n")
		return
	}

	var needSpace bool

	// Level.
	if !s.noLevel {
		s.Formatter.FormatLevel(&s.state, l.lvl, w)
		w.WriteString(" ")
	}

	// Time.
	if !s.noTime {
		s.Formatter.FormatTime(&s.state, l.time, w)
		w.WriteString(" ")
	}

	// Message.
	if !s.noMessage {
		s.Formatter.FormatMessage(&s.state, l.msg, l.highlight, w)
		needSpace = true
	}

	seenFieldsThisIteration := make(map[string]struct{})
	write := func(k string, v interface{}) {
		if needSpace {
			w.WriteString(" ")
		}
		seenFieldsThisIteration[k] = struct{}{}
		delete(l.fields, k)
		s.Formatter.FormatField(&s.state, k, v, w)
		needSpace = true
	}

	// Fields the user explicitly wants to see.
	for _, k := range s.PriorityFields {
		if v, ok := l.fields[k]; ok {
			write(k, v)
		}
	}

	// Fields we've seen on past lines.
	for _, k := range s.state.seenFields {
		if v, ok := l.fields[k]; ok {
			write(k, v)
		}
	}

	// Any new fields (in a deterministic order, mostly for tests).
	newFields := make([]string, 0, len(l.fields))
	for k := range l.fields {
		newFields = append(newFields, k)
	}
	sort.Strings(newFields)
	for _, k := range newFields {
		v := l.fields[k]
		write(k, v)
		s.state.seenFields = append(s.state.seenFields, k)
	}

	// Keep state for field eliding.
	for k := range s.state.lastFields {
		if _, ok := seenFieldsThisIteration[k]; !ok {
			delete(s.state.lastFields, k)
		}
	}

	// Final newline is our responsibility.
	w.WriteString("\n")
}
