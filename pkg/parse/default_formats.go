package parse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	aurora "github.com/logrusorgru/aurora/v3"
)

type DefaultOutputFormatter struct {
	Aurora               aurora.Aurora // Controls the use of color.
	ElideDuplicateFields bool          // If true, print ↑↑↑ for fields that have an identical value as the previous line.
	AbsoluteTimeFormat   string        // If true, print relative timestamps instead of absolute timestamps.
	TimePrecision        time.Duration // Precicion to truncate relative timestamps to.
	AbsoluteEvery        int           // Print absolute timestamps every this number of lines, with other lines being the delta since the last absolute timestamp.
}

var programStartTime = time.Now()

func (f *DefaultOutputFormatter) FormatTime(s *State, t time.Time, w io.Writer) error {
	var out string
	switch {
	case t.IsZero():
		out = "???"
	case f.AbsoluteEvery > 0:
		if s.lastTime.IsZero() || s.linesSinceLastTimePrinted >= f.AbsoluteEvery-1 {
			out = t.In(time.Local).Format(f.AbsoluteTimeFormat)
			s.linesSinceLastTimePrinted = 0
			s.lastTime = t
		} else {
			out = "+" + t.Sub(s.lastTime).Truncate(time.Microsecond).String()
			for utf8.RuneCountInString(out) < s.timePadding {
				out = " " + out
			}
			s.linesSinceLastTimePrinted++
		}
	case f.AbsoluteTimeFormat != "":
		out = t.In(time.Local).Format(f.AbsoluteTimeFormat)
	default:
		out = programStartTime.Sub(t).Truncate(f.TimePrecision).String()
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

func (f *DefaultOutputFormatter) FormatLevel(s *State, level string, w io.Writer) error {
	var l aurora.Value
	switch strings.ToLower(level) {
	case "debug":
		l = f.Aurora.Blue("DEBUG")
	case "info":
		l = f.Aurora.Cyan("INFO ")
	case "warn":
		l = f.Aurora.Yellow("WARN ")
	case "error":
		l = f.Aurora.Red("ERR  ")
	case "panic":
		l = f.Aurora.Magenta("PANIC")
	case "dpanic":
		l = f.Aurora.Magenta("DPANI")
	case "fatal":
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
