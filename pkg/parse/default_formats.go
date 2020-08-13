package parse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	aurora "github.com/logrusorgru/aurora/v3"
)

type DefaultOutputFormatter struct {
	Aurora               aurora.Aurora // Controls the use of color.
	ElideDuplicateFields bool          // If true, print ↑↑↑ for fields that have an identical value as the previous line.
	AbsoluteTimeFormat   string        // If true, print relative timestamps instead of absolute timestamps.
}

var programStartTime = time.Now()

func (f *DefaultOutputFormatter) FormatTime(s *State, t time.Time, w io.Writer) error {
	var out string
	switch {
	case t.IsZero():
		out = "???"
		for utf8.RuneCountInString(out) < s.timePadding {
			out = " " + out
		}
	case f.AbsoluteTimeFormat == "":
		rel := t.Sub(programStartTime)
		abs := rel
		if rel < 0 {
			abs = -rel
		}
		var p time.Duration
		switch {
		case abs < time.Microsecond:
			p = time.Nanosecond
		case abs < time.Millisecond:
			p = time.Microsecond
		case abs < time.Second:
			p = time.Millisecond
		default:
			p = time.Second
		}
		out = rel.Truncate(p).String()
	default:
		out = t.In(time.Local).Format(f.AbsoluteTimeFormat)
	}
	for utf8.RuneCountInString(out) < s.timePadding {
		out += " "
	}
	if l := utf8.RuneCountInString(out); l > s.timePadding {
		s.timePadding = l
	}
	_, err := w.Write([]byte(f.Aurora.Green(out).String()))
	return err
}

func (f *DefaultOutputFormatter) FormatMessage(s *State, msg string, w io.Writer) error {
	_, err := w.Write([]byte(msg))
	return err
}

func (f *DefaultOutputFormatter) FormatLevel(s *State, level Level, w io.Writer) error {
	var l aurora.Value
	switch level {
	case LevelTrace:
		l = f.Aurora.Gray(15, "TRACE")
	case LevelDebug:
		l = f.Aurora.Blue("DEBUG")
	case LevelInfo:
		l = f.Aurora.Cyan("INFO ")
	case LevelWarn:
		l = f.Aurora.Yellow("WARN ")
	case LevelError:
		l = f.Aurora.Red("ERROR")
	case LevelPanic:
		l = f.Aurora.Magenta("PANIC")
	case LevelDPanic:
		l = f.Aurora.Magenta("DPANI")
	case LevelFatal:
		l = f.Aurora.BgMagenta("FATAL")
	default:
		l = f.Aurora.Gray(15, "UNK  ")
	}
	_, err := w.Write([]byte(l.String()))
	return err
}

func (f *DefaultOutputFormatter) FormatField(s *State, k string, v interface{}, w io.Writer) error {
	if _, err := w.Write([]byte(f.Aurora.Gray(16, k+":").String())); err != nil {
		return fmt.Errorf("write key: %w", err)
	}

	var value []byte
	switch x := v.(type) {
	case string:
		value = []byte(x)
	default:
		var err error
		value, err = json.Marshal(v)
		if err != nil {
			return fmt.Errorf("marshal value: %w", err)
		}
	}

	if f.ElideDuplicateFields {
		old, ok := s.lastFields[k]
		if ok && bytes.Equal(old, value) {
			s.lastFields[k] = value
			value = []byte("↑")
		} else {
			s.lastFields[k] = value
		}
	}

	if _, err := w.Write(value); err != nil {
		return fmt.Errorf("write value: %w", err)
	}
	return nil
}
