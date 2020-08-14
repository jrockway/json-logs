package parse

import (
	"bufio"
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

// InputSchema controls the intepretation of incoming log lines.
type InputSchema struct {
	TimeKey     string      // The name of the key that holds the timestamp.
	TimeFormat  TimeParser  // How to turn the value of the time key into a time.Time.
	LevelKey    string      // The name of the key that holds the log level.
	LevelFormat LevelParser // How to turn the value of the level key into a Level.
	MessageKey  string      // The name of the key that holds the main log message.

	// If true, print an error when non-JSON lines appear in the input.  If false, treat them
	// as normal messages with as much information extracted as possible.
	Strict bool
}

// OutputFormatter describes an object that actually does the output formatting.
type OutputFormatter interface {
	// FormatTime is a function that formats a time.Time and outputs it to an io.Writer.
	FormatTime(s *State, t time.Time, w io.Writer) error

	// FormatLevel is a function that formats a log level and outputs it to an io.Writer.
	FormatLevel(s *State, lvl Level, w io.Writer) error

	// FormatMessage is a function that formats a log message and outputs it to an io.Writer.
	FormatMessage(s *State, msg string, w io.Writer) error

	// FormatField is a function that formats a (key, value) pair and outputs it to an io.Writer.
	FormatField(s *State, k string, v interface{}, w io.Writer) error
}

// State keeps state between log lines.
type State struct {
	// seenFields maintains an ordering of all fields, so that they are consistent between log lines.
	seenFields                []string
	timePadding               int
	lastFields                map[string][]byte
	lastTime                  time.Time
	linesSinceLastTimePrinted int
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
	time   time.Time
	msg    string
	lvl    Level
	raw    []byte
	err    error
	fields map[string]interface{}
}

func (l *line) pushError(err error) {
	if l.err == nil {
		l.err = err
		return
	}
	l.err = fmt.Errorf("%v; %v", l.err, err)
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
			l.fields = x
		case nil:
			return false, errors.New("unexpected nil result; yield an empty map ('{}') to delete all fields")
		case error:
			return false, fmt.Errorf("error: %w", x)
		case bool:
			return false, errors.New("unexpected boolean output; did you mean to use 'select(...)'?")
		default:
			return false, fmt.Errorf("unexpected result type %T(%#v) ", result, result)
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
	for s.Scan() {
		sum.Lines++
		// Clear line.
		l.raw = s.Bytes()
		l.err = nil
		l.msg = ""
		l.fields = make(map[string]interface{})
		l.lvl = LevelUnknown
		l.time = time.Time{}

		// Parse input.
		ins.ReadLine(&l)

		// Filter.
		filtered, err := runJQ(jq, &l)
		if err != nil {
			sum.Errors++
			// It is questionable as to whether or not jq breaking means that we should
			// stop processing the log entirely.  It's probably a bug that affects every
			// line, so we return the error rather than l.pushError() to display the
			// error and keep processing.
			return sum, fmt.Errorf("jq: %w", err)
		}
		if filtered {
			sum.Filtered++
			continue
		}

		// Emit.
		err = outs.Emit(w, &l)
		if l.err != nil || err != nil {
			sum.Errors++
		}
		if err != nil {
			return sum, fmt.Errorf("emit: line %d: %w", sum.Lines, err)
		}
	}
	return sum, s.Err()
}

// ReadLine parses a log line into the provided line object.
func (s *InputSchema) ReadLine(l *line) {
	if !s.Strict && ((len(l.raw) > 0 && l.raw[0] != '{') || len(l.raw) == 0) {
		l.time = time.Time{}
		l.msg = string(l.raw)
		return
	}
	if err := json.Unmarshal(l.raw, &l.fields); err != nil {
		l.pushError(fmt.Errorf("unmarshal json: %w", err))
		if !s.Strict {
			l.msg = string(l.raw)
		}
	}
	if t, ok := l.fields[s.TimeKey]; s.TimeFormat != nil && ok {
		time, err := s.TimeFormat(t)
		if err != nil {
			l.pushError(fmt.Errorf("parse time %T(%v) in key %q: %w", t, t, s.TimeKey, err))
		} else {
			delete(l.fields, s.TimeKey)
			l.time = time
		}
	} else {
		l.pushError(fmt.Errorf("no time key %q in incoming log", s.TimeKey))
	}
	if msg, ok := l.fields[s.MessageKey]; ok {
		switch x := msg.(type) {
		case string:
			l.msg = x
			delete(l.fields, s.MessageKey)
		case []byte:
			l.msg = string(x)
			delete(l.fields, s.MessageKey)
		default:
			l.msg = string(l.raw)
			l.pushError(fmt.Errorf("message key %q contains non-string data (%q of type %T)", s.MessageKey, msg, msg))
		}
	} else {
		l.pushError(fmt.Errorf("no message key %q in incoming log", s.MessageKey))
	}
	if lvl, ok := l.fields[s.LevelKey]; ok {
		if parsed, err := s.LevelFormat(lvl); err != nil {
			l.pushError(fmt.Errorf("level key %q: %w", s.LevelKey, err))
		} else {
			l.lvl = parsed
			delete(l.fields, s.LevelKey)
		}
	} else {
		l.pushError(fmt.Errorf("no level key %q in incoming log", s.LevelKey))
	}
	if !s.Strict {
		l.err = nil
	}
}

// Emit emits a formatted line to the provided io.Writer.  The provided line object may not be used
// again until reinitalized.
func (s *OutputSchema) Emit(w io.Writer, l *line) (retErr error) {
	defer func() {
		if err := recover(); err != nil {
			w.Write([]byte("\n"))
			buf := make([]byte, 2048)
			runtime.Stack(buf, false)
			retErr = errors.New(fmt.Sprintf("%s\n%s", err, buf))
		}
	}()
	if l.err != nil {
		if _, err := w.Write(l.raw); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
		s.EmitError(l.err.Error())
		return nil
	}

	// Level.
	if err := s.Formatter.FormatLevel(&s.state, l.lvl, w); err != nil {
		return err
	}
	// We don't check for an error here (and on similar writes) because no information is being
	// lost by the whitespace failing to be written.  You'll know.
	w.Write([]byte(" "))

	// Time.
	if err := s.Formatter.FormatTime(&s.state, l.time, w); err != nil {
		return err
	}
	w.Write([]byte(" "))

	// Message.
	if err := s.Formatter.FormatMessage(&s.state, l.msg, w); err != nil {
		return err
	}

	seenFieldsThisIteration := make(map[string]struct{})

	// Fields the user explicitly wants to see.
	for _, k := range s.PriorityFields {
		if v, ok := l.fields[k]; ok {
			seenFieldsThisIteration[k] = struct{}{}
			w.Write([]byte(" "))
			delete(l.fields, k)
			if err := s.Formatter.FormatField(&s.state, k, v, w); err != nil {
				return err
			}
		}
	}

	// Fields we've seen on past lines.
	for _, k := range s.state.seenFields {
		if v, ok := l.fields[k]; ok {
			seenFieldsThisIteration[k] = struct{}{}
			w.Write([]byte(" "))
			delete(l.fields, k)
			if err := s.Formatter.FormatField(&s.state, k, v, w); err != nil {
				return err
			}
		}
	}

	// Any new fields.
	for k, v := range l.fields {
		seenFieldsThisIteration[k] = struct{}{}
		w.Write([]byte(" "))
		s.state.seenFields = append(s.state.seenFields, k)
		delete(l.fields, k)
		if err := s.Formatter.FormatField(&s.state, k, v, w); err != nil {
			return err
		}
	}

	for k := range s.state.lastFields {
		if _, ok := seenFieldsThisIteration[k]; !ok {
			delete(s.state.lastFields, k)
		}
	}

	// Final newline is our responsibility.
	w.Write([]byte("\n"))
	return nil
}
