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
	"time"

	"github.com/itchyny/gojq"
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

// InputSchema controls the interpretation of incoming log lines.
type InputSchema struct {
	TimeKey     string      // The name of the key that holds the timestamp.
	TimeFormat  TimeParser  // How to turn the value of the time key into a time.Time.
	LevelKey    string      // The name of the key that holds the log level.
	LevelFormat LevelParser // How to turn the value of the level key into a Level.
	MessageKey  string      // The name of the key that holds the main log message.

	// If true, print an error when non-JSON lines appear in the input.  If false, treat them
	// as normal messages with as much information extracted as possible.
	Strict bool

	// DeleteKeys contains a list of keys to delete; used when the log lines contain version
	// information that is used for guessing the schema.
	DeleteKeys []string
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
	// seenFields maintains an ordering of all fields, so that they are consistent between log lines.
	seenFields  []string
	timePadding int
	lastFields  map[string][]byte
	lastTime    time.Time
}

// OutputSchema controls how output lines are formatted.
type OutputSchema struct {
	PriorityFields []string         // PriorityFields controls which fields are printed first.
	Formatter      OutputFormatter  // Actually does the formatting.
	EmitErrorFn    func(msg string) // A function that sees all errors.
	state          State            // state carries context between lines
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
	time      time.Time
	msg       string
	lvl       Level
	raw       []byte
	highlight bool
	fields    map[string]interface{}
}

type Summary struct {
	Lines    int
	Errors   int
	Filtered int
}

var DefaultVariables = []string{
	"$TS",
	"$RAW", "$MSG",
	"$LVL", "$UNKNOWN", "$TRACE", "$DEBUG", "$INFO", "$WARN", "$ERROR", "$PANIC", "$DPANIC", "$FATAL",
}

func prepareVariables(l *line) []interface{} {
	return []interface{}{
		float64(l.time.UnixNano()) / 1e9, // $TS
		string(l.raw), l.msg,
		uint8(l.lvl), uint8(LevelUnknown), uint8(LevelTrace), uint8(LevelDebug), uint8(LevelInfo), uint8(LevelWarn), uint8(LevelError), uint8(LevelPanic), uint8(LevelDPanic), uint8(LevelFatal),
	}
}

const highlightKey = "__highlight"

// CompileJQ compiles the provided jq program.
func CompileJQ(p string) (*gojq.Code, error) {
	if p == "" {
		return nil, nil
	}
	p = "def highlight($cond): . + {__highlight: $cond};\n" + p
	q, err := gojq.Parse(p)
	if err != nil {
		return nil, fmt.Errorf("parsing jq program %q: %v", p, err)
	}
	jq, err := gojq.Compile(q, gojq.WithVariables(DefaultVariables))
	if err != nil {
		return nil, fmt.Errorf("compiling jq program %q: %v", p, err)
	}
	return jq, nil
}

// runJQ runs the provided jq program on the provided line.  It returns true if the result is empty
// (i.e., the line should be filtered out), and an error if the output type is invalid or another
// error occurred.
func runJQ(jq *gojq.Code, l *line) (bool, error) {
	if jq == nil {
		return false, nil
	}
	var filtered bool
	iter := jq.Run(l.fields, prepareVariables(l)...)
	if result, ok := iter.Next(); ok {
		switch x := result.(type) {
		case map[string]interface{}:
			if raw, ok := x[highlightKey]; ok {
				delete(x, highlightKey)
				if hi, ok := raw.(bool); ok {
					l.highlight = hi
				}
			}
			l.fields = x
		case nil:
			return false, errors.New("unexpected nil result; yield an empty map ('{}') to delete all fields")
		case error:
			return false, fmt.Errorf("error: %w", x)
		case bool:
			return false, errors.New("unexpected boolean output; did you mean to use 'select(...)'?")
		default:
			return false, fmt.Errorf("unexpected result type %T(%#v)", result, result)
		}
		if _, ok = iter.Next(); ok {
			// We only use the first line that is output.  This can be revisited in the
			// future.
			return false, errors.New("unexpectedly produced more than 1 output")
		}
	} else {
		filtered = true
		l.fields = make(map[string]interface{})
	}
	return filtered, nil
}

// ReadLog reads a stream of JSON-formatted log lines from the provided reader according to the
// input schema, reformatting it and writing to the provided writer according to the output schema.
// Parse errors are handled according to the input schema.  Any other errors, not including io.EOF
// on the reader, are returned.
func ReadLog(r io.Reader, w io.Writer, ins *InputSchema, outs *OutputSchema, jq *gojq.Code) (Summary, error) {
	s := bufio.NewScanner(r)
	var l line
	outs.state = State{
		lastFields: make(map[string][]byte),
	}
	if outs.Formatter == nil {
		outs.Formatter = &DefaultOutputFormatter{}
	}
	var sum Summary
	buf := new(bytes.Buffer)
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
			buf.Truncate(0)
			l.raw = s.Bytes()
			l.msg = ""
			l.fields = make(map[string]interface{})
			l.lvl = LevelUnknown
			l.time = time.Time{}
			l.highlight = false

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
			filtered, err := runJQ(jq, &l)
			if err != nil {
				addError = true
				writeRawLine = true
				recoverable = false
				// It is questionable as to whether or not jq breaking means that we
				// should stop processing the log entirely.  It's probably a bug in
				// the jq program that affects every line, so the sooner we return
				// the error, the sooner the user can fix their program.  But on the
				// other hand, is it worth it to spend the time debugging a jq
				// program that's only broken on one line out of a billion?
				return fmt.Errorf("jq: %w", err)
			}
			if filtered {
				sum.Filtered++
				if parseErr != nil {
					addError = true
					recoverable = true
					writeRawLine = false
					return fmt.Errorf("parse: %w", parseErr)
				}
				return nil
			}

			// Emit a line to the buffer.
			outs.Emit(&l, buf)

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
		return
	}
	if len(l.fields) == 5 && has("timestamp") && has("log_level") && has("message") && has("data") && has("source") {
		// lager non-pretty
		s.TimeKey = "timestamp"
		s.TimeFormat = StrictUnixTimeParser
		s.LevelKey = "log_level"
		s.LevelFormat = LagerLevelParser
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
	if lvl, ok := l.fields[s.LevelKey]; ok {
		if parsed, err := s.LevelFormat(lvl); err != nil {
			pushError(fmt.Errorf("level key %q: %w", s.LevelKey, err))
		} else {
			l.lvl = parsed
			delete(l.fields, s.LevelKey)
		}
	} else {
		pushError(fmt.Errorf("no level key %q in incoming log", s.LevelKey))
	}
	for _, k := range s.DeleteKeys {
		delete(l.fields, k)
	}
	return retErr
}

// Emit emits a formatted line to the provided buffer.  The provided line object may not be used
// again until reinitialized.
func (s *OutputSchema) Emit(l *line, w *bytes.Buffer) {
	// Level.
	s.Formatter.FormatLevel(&s.state, l.lvl, w)
	w.WriteString(" ")

	// Time.
	s.Formatter.FormatTime(&s.state, l.time, w)
	w.WriteString(" ")

	// Message.
	s.Formatter.FormatMessage(&s.state, l.msg, l.highlight, w)

	seenFieldsThisIteration := make(map[string]struct{})

	// Fields the user explicitly wants to see.
	for _, k := range s.PriorityFields {
		if v, ok := l.fields[k]; ok {
			seenFieldsThisIteration[k] = struct{}{}
			w.WriteString(" ")
			delete(l.fields, k)
			s.Formatter.FormatField(&s.state, k, v, w)
		}
	}

	// Fields we've seen on past lines.
	for _, k := range s.state.seenFields {
		if v, ok := l.fields[k]; ok {
			seenFieldsThisIteration[k] = struct{}{}
			w.WriteString(" ")
			delete(l.fields, k)
			s.Formatter.FormatField(&s.state, k, v, w)
		}
	}

	// Any new fields.
	for k, v := range l.fields {
		seenFieldsThisIteration[k] = struct{}{}
		w.WriteString(" ")
		s.state.seenFields = append(s.state.seenFields, k)
		delete(l.fields, k)
		s.Formatter.FormatField(&s.state, k, v, w)
	}

	for k := range s.state.lastFields {
		if _, ok := seenFieldsThisIteration[k]; !ok {
			delete(s.state.lastFields, k)
		}
	}

	// Final newline is our responsibility.
	w.WriteString("\n")
}
