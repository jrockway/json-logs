package parse

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	aurora "github.com/logrusorgru/aurora/v3"
)

type DefaultOutputFormatter struct {
	Aurora               aurora.Aurora // Controls the use of color.
	ElideDuplicateFields bool          // If true, print ↑↑↑ for fields that have an identical value as the previous line.
	RelativeTimes        bool          // If true, print relative timestamps instead of absolute timestamps.
	TimePrecision        time.Duration // Precicion to truncate timestamps to.
}

var programStartTime = time.Now()

func (f *DefaultOutputFormatter) FormatTime(s *State, t time.Time, w io.Writer) error {
	var out string
	switch {
	case t.IsZero():
		out = "???"
	default:
		out = programStartTime.Sub(t).Truncate(f.TimePrecision).String()
	}
	for len(out) < s.timePadding {
		out += " "
	}
	if l := len(out); l > s.timePadding {
		s.timePadding = l
	}
	_, err := w.Write([]byte(f.Aurora.Cyan(out).String()))
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
	if _, err := w.Write([]byte(f.Aurora.Gray(5, k+":").String())); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	if str, ok := v.(string); ok {
		if _, err := w.Write([]byte(str)); err != nil {
			return fmt.Errorf("write string value: %w", err)
		}
		return nil
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
