package parse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strings"
	"time"
)

type state struct {
	// seenFields maintains an ordering of all fields, so that they are consistent between log lines.
	seenFields  []string
	timePadding int
}

type (
	// TimeParser is a function that parses timestamps in log messages.
	TimeParser func(interface{}) (time.Time, error)

	// TimeFormatter is a function that formats a time.Time and outputs it to an io.Writer.
	TimeFormatter func(*state, time.Time, io.Writer) error

	// LevelFormatter is a function that formats a log level and outputs it to an io.Writer.
	LevelFormatter func(*state, string, io.Writer) error

	// MessageFormatter is a function that formats a log message and outputs it to an io.Writer.
	MessageFormatter func(*state, string, io.Writer) error

	// FieldFormatter is a function that formats a (key, value) pair and outputs it to an io.Writer.
	FieldFormatter func(*state, string, interface{}, io.Writer) error
)

// InputSchema controls the intepretation of incoming log lines.
type InputSchema struct {
	TimeKey    string     // The name of the key that holds the timestamp.
	TimeFormat TimeParser // How to turn the value of the time key into a time.Time.
	MessageKey string     // The name of the key that holds the main log message.

	// If true, produce an error when non-JSON lines appear in the input.  If false, treat them
	// as normal messages that happened at an unknown time.
	StrictObject bool
}

// OutputSchema controls how output lines are formatted.
type OutputSchema struct {
	LevelKey        string           // LevelKey names the log level.
	PriorityFields  []string         // PriorityFields controls which fields are printed first.
	TimeFormatFn    TimeFormatter    // TimeFormatFn emits formatted timestamps.
	LevelFormatFn   LevelFormatter   // LevelFormatFn formats the log level.
	MessageFormatFn MessageFormatter // MessageFormatFn emits formatted log messages.
	FieldFormatFn   FieldFormatter   // FieldFormatFn emits formatted key,value pairs.
	EmitErrorFn     func(msg string) // A function that sees all errors.
	state           state            // state carries context between lines
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

func DefaultTimeParserFn(in interface{}) (time.Time, error) {
	var sec, nsec int64
	switch x := in.(type) {
	case int:
		sec = int64(x)
		nsec = 0
	case int64:
		sec = x
		nsec = 0
	case float64:
		sec = int64(math.Floor(x))
		nsec = 1_000_000_000 * int64(x-math.Floor(x))
	default:
		return time.Time{}, errors.New("invalid time format")
	}
	return time.Unix(sec, nsec), nil
}

var programStartTime = time.Now()

func DefaultTimeFormatFn(s *state, t time.Time, w io.Writer) error {
	var f string
	switch {
	case t.IsZero():
		f = "???"
	default:
		f = programStartTime.Sub(t).Truncate(time.Millisecond).String()
	}
	for len(f) < s.timePadding {
		f += " "
	}
	if l := len(f); l > s.timePadding {
		s.timePadding = l
	}
	_, err := w.Write([]byte(f))
	return err
}

func DefaultMessageFormatFn(s *state, msg string, w io.Writer) error {
	_, err := w.Write([]byte(msg))
	return err
}

func DefaultLevelFormatFn(s *state, level string, w io.Writer) error {
	_, err := w.Write([]byte(strings.ToUpper(level)))
	return err
}

func DefaultFieldFormatFn(s *state, k string, v interface{}, w io.Writer) error {
	if _, err := w.Write([]byte(k + ":")); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	value, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}
	if _, err := w.Write(value); err != nil {
		return fmt.Errorf("write value: %w", err)
	}
	return nil
}

// ReadLog reads a stream of JSON-formatted log lines from the provided reader according to the
// input schema, reformatting it and writing to the provided writer according to the output schema.
// Parse errors are handled according to the input schema.  Any other errors, not including io.EOF
// on the reader, are returned.
func ReadLog(r io.Reader, w io.Writer, ins *InputSchema, outs *OutputSchema) error {
	s := bufio.NewScanner(r)
	var l line
	outs.state = state{}

	if outs.TimeFormatFn == nil {
		outs.TimeFormatFn = DefaultTimeFormatFn
	}
	if outs.LevelFormatFn == nil {
		outs.LevelFormatFn = DefaultLevelFormatFn
	}
	if outs.MessageFormatFn == nil {
		outs.MessageFormatFn = DefaultMessageFormatFn
	}
	if outs.FieldFormatFn == nil {
		outs.FieldFormatFn = DefaultFieldFormatFn
	}

	for s.Scan() {
		l.n++
		l.raw = s.Bytes()
		l.err = nil
		l.msg = ""
		l.fields = make(map[string]interface{})
		ins.ReadLine(&l)
		if err := outs.Emit(w, &l); err != nil {
			return fmt.Errorf("emit: line %d: %w", l.n, err)
		}
	}
	return s.Err()
}

// ReadLine parses a log line into the provided line object.
func (s *InputSchema) ReadLine(l *line) {
	if !s.StrictObject && len(l.raw) > 0 && l.raw[0] != '{' {
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
			l.pushError(fmt.Errorf("parse time %q in key %q: %w", t, s.TimeKey, err))
		} else {
			delete(l.fields, s.TimeKey)
			l.time = time
		}
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
			l.pushError(fmt.Errorf("message key %q contains non-string data (%q of type %t)", s.MessageKey, msg, msg))
		}
	}
	// Should we warn if the message key doesn't contain a message?  It means the schema is
	// probably wrong.
}

// Emit emits a formatted line to the provided io.Writer.  The provided line object may not be used
// again until reinitalized.
func (s *OutputSchema) Emit(w io.Writer, l *line) error {
	defer func() {
		if err := recover(); err != nil {
			s.EmitError(fmt.Sprintf("emit: panic: %s", err))
			buf := make([]byte, 2048)
			n := runtime.Stack(buf, false)
			for _, l := range bytes.Split(buf[0:n], []byte("\n")) {
				s.EmitError(string(l))
			}
		}
	}()
	if l.err != nil {
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
	if err := s.TimeFormatFn(&s.state, l.time, w); err != nil {
		errs = append(errs, fmt.Errorf("write time %q: %w", l.time.Format(time.RFC3339), err))
	}
	w.Write([]byte(" "))
	// we don't check for an error here because no information is being lost by the whitespace
	// failing to be written.
	if lvl, ok := l.fields[s.LevelKey]; ok {
		if _, ok := lvl.(string); ok {
			delete(l.fields, s.LevelKey)
			if err := s.LevelFormatFn(&s.state, lvl.(string), w); err != nil {
				errs = append(errs, fmt.Errorf("write level: %w", err))
			}
			w.Write([]byte(" "))
		}
	}
	if err := s.MessageFormatFn(&s.state, l.msg, w); err != nil {
		errs = append(errs, fmt.Errorf("write message %q: %w", l.msg, err))
	}
	for _, k := range s.PriorityFields {
		if v, ok := l.fields[k]; ok {
			w.Write([]byte(" "))
			delete(l.fields, k)
			if err := s.FieldFormatFn(&s.state, k, v, w); err != nil {
				errs = append(errs, fmt.Errorf("write field %q: %w", k, err))
			}
		}
	}
	for _, k := range s.state.seenFields {
		if v, ok := l.fields[k]; ok {
			w.Write([]byte(" "))
			delete(l.fields, k)
			if err := s.FieldFormatFn(&s.state, k, v, w); err != nil {
				errs = append(errs, fmt.Errorf("write field %q: %w", k, err))
			}
		}
	}
	for k, v := range l.fields {
		w.Write([]byte(" "))
		s.state.seenFields = append(s.state.seenFields, k)
		delete(l.fields, k)
		if err := s.FieldFormatFn(&s.state, k, v, w); err != nil {
			errs = append(errs, fmt.Errorf("write field %q: %w", k, err))
		}
	}
	w.Write([]byte("\n"))
	for _, err := range errs {
		s.EmitError(err.Error())
	}
	if len(errs) > 0 {
		return errors.New("write error; details written to debug log")
	}
	return nil
}
