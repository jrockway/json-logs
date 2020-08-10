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

// InputSchema controls the intepretation of incoming log lines.
type InputSchema struct {
	TimeKey    string     // The name of the key that holds the timestamp.
	TimeFormat TimeParser // How to turn the value of the time key into a time.Time.
	LevelKey   string     // The name of the key that holds the log level.
	MessageKey string     // The name of the key that holds the main log message.

	// If true, print an error when non-JSON lines appear in the input.  If false, treat them
	// as normal messages with as much information extracted as possible.
	Strict bool
}

// State keeps state between log lines.
type State struct {
	// seenFields maintains an ordering of all fields, so that they are consistent between log lines.
	seenFields                []string
	timePadding               int
	lastFields                map[string][]byte
	lastTime                  time.Time
	linesSinceLastTimePrinted int
	linesWithErrors           int
	totalLines                int
}

// OutputFormatter describes an object that actually does the output formatting.
type OutputFormatter interface {
	// FormatTime is a function that formats a time.Time and outputs it to an io.Writer.
	FormatTime(s *State, t time.Time, w io.Writer) error

	// FormatLevel is a function that formats a log level and outputs it to an io.Writer.
	FormatLevel(s *State, lvl string, w io.Writer) error

	// FormatMessage is a function that formats a log message and outputs it to an io.Writer.
	FormatMessage(s *State, msg string, w io.Writer) error

	// FormatField is a function that formats a (key, value) pair and outputs it to an io.Writer.
	FormatField(s *State, k string, v interface{}, w io.Writer) error
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
		os.Stderr.WriteString("    ↳ " + msg + "\n")
	} else {
		s.EmitErrorFn(msg)
	}
}

// line represents one log line.
type line struct {
	n      int
	time   time.Time
	msg    string
	lvl    string
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

// ReadLog reads a stream of JSON-formatted log lines from the provided reader according to the
// input schema, reformatting it and writing to the provided writer according to the output schema.
// Parse errors are handled according to the input schema.  Any other errors, not including io.EOF
// on the reader, are returned.
func ReadLog(r io.Reader, w io.Writer, ins *InputSchema, outs *OutputSchema, jq []*gojq.Code) (Summary, error) {
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
		l.n++
		l.raw = s.Bytes()
		l.err = nil
		l.msg = ""
		l.fields = make(map[string]interface{})
		l.lvl = ""
		ins.ReadLine(&l)
		now := float64(time.Now().UnixNano()) / 1e9
		ts := float64(l.time.UnixNano()) / 1e9
		var filtered bool
		for _, p := range jq {
			iter := p.Run(l.fields, now, ts, l.msg, l.lvl)
			result, ok := iter.Next()
			if ok {
				// We only use the first line that is output.  This can be revisited in the
				// future.
				switch x := result.(type) {
				case error:
					l.pushError(fmt.Errorf("error from jq program: %v", x))
				case map[string]interface{}:
					l.fields = x
				default:
					l.pushError(fmt.Errorf("unexpected result %T(%#v) from jq program", result, result))
				}
				_, ok = iter.Next()
				if ok {
					l.pushError(errors.New("jq program unexpectedly produced more than 1 line of output; this is currently unsupported"))
				}
			} else {
				filtered = true
				l.fields = make(map[string]interface{})
				break
			}
		}
		if filtered && len(l.fields) == 0 {
			sum.Filtered++
			continue
		}
		err := outs.Emit(w, &l)
		sum.Lines = outs.state.totalLines
		sum.Errors = outs.state.linesWithErrors
		if err != nil {
			return sum, fmt.Errorf("emit: line %d: %w", l.n, err)
		}
	}
	return sum, s.Err()
}

// ReadLine parses a log line into the provided line object.
func (s *InputSchema) ReadLine(l *line) {
	if !s.Strict && len(l.raw) > 0 && l.raw[0] != '{' {
		l.time = time.Time{}
		l.msg = string(l.raw)
		return
	}
	if err := json.Unmarshal(l.raw, &l.fields); err != nil {
		l.pushError(fmt.Errorf("unmarshal json: %w", err))
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
		switch x := lvl.(type) {
		case string:
			l.lvl = x
			delete(l.fields, s.LevelKey)
		case []byte:
			l.lvl = string(x)
			delete(l.fields, s.LevelKey)
		default:
			l.pushError(fmt.Errorf("level key %q contains non-string data (%q of type %T)", s.LevelKey, lvl, lvl))
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
	s.state.totalLines++
	defer func() {
		if err := recover(); err != nil {
			s.state.linesWithErrors++
			w.Write([]byte("\n"))
			buf := make([]byte, 2048)
			runtime.Stack(buf, false)
			retErr = errors.New(fmt.Sprintf("%s\n%s", err, buf))
		}
	}()
	if l.err != nil {
		s.state.linesWithErrors++
		ok := true
		if _, err := w.Write(l.raw); err != nil {
			ok = false
			s.EmitError(err.Error())
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			ok = false
			s.EmitError(err.Error())
		}
		s.EmitError(l.err.Error())
		if !ok {
			return errors.New("write error; details written to debug log")
		}
		return nil
	}
	var errs []error

	// Level.
	if err := s.Formatter.FormatLevel(&s.state, l.lvl, w); err != nil {
		errs = append(errs, fmt.Errorf("write level: %w", err))
	}
	// We don't check for an error here (and on similar writes) because no information is being
	// lost by the whitespace failing to be written.  You'll know.
	w.Write([]byte(" "))

	// Time.
	if err := s.Formatter.FormatTime(&s.state, l.time, w); err != nil {
		errs = append(errs, fmt.Errorf("write time %q: %w", l.time.Format(time.RFC3339), err))
	}
	w.Write([]byte(" "))

	// Message.
	if err := s.Formatter.FormatMessage(&s.state, l.msg, w); err != nil {
		errs = append(errs, fmt.Errorf("write message %q: %w", l.msg, err))
	}

	seenFieldsThisIteration := make(map[string]struct{})

	// Fields the user explicitly wants to see.
	for _, k := range s.PriorityFields {
		if v, ok := l.fields[k]; ok {
			seenFieldsThisIteration[k] = struct{}{}
			w.Write([]byte(" "))
			delete(l.fields, k)
			if err := s.Formatter.FormatField(&s.state, k, v, w); err != nil {
				errs = append(errs, fmt.Errorf("write field %q: %w", k, err))
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
				errs = append(errs, fmt.Errorf("write field %q: %w", k, err))
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
			errs = append(errs, fmt.Errorf("write field %q: %w", k, err))
		}
	}

	for k := range s.state.lastFields {
		if _, ok := seenFieldsThisIteration[k]; !ok {
			delete(s.state.lastFields, k)
		}
	}
	// If there were warnings, print them.
	for _, err := range errs {
		s.EmitError(err.Error())
	}
	if len(errs) > 0 {
		s.state.linesWithErrors++
		return errors.New("write error; details written to debug log")
	}

	w.Write([]byte("\n"))
	return nil
}
