package parse

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/itchyny/gojq"
)

// FilterScheme controls how lines are filtered.
type FilterScheme struct {
	JQ           *gojq.Code
	MatchRegex   *regexp.Regexp
	NoMatchRegex *regexp.Regexp
}

// DefaultVariables are variables available to JQ programs.
var DefaultVariables = []string{
	"$TS",
	"$RAW", "$MSG",
	"$LVL", "$UNKNOWN", "$TRACE", "$DEBUG", "$INFO", "$WARN", "$ERROR", "$PANIC", "$DPANIC", "$FATAL",
}

// prepareVariable extracts the variables above from a line.
func prepareVariables(l *line) []interface{} {
	return []interface{}{
		float64(l.time.UnixNano()) / 1e9, // $TS
		string(l.raw), l.msg,
		uint8(l.lvl), uint8(LevelUnknown), uint8(LevelTrace), uint8(LevelDebug), uint8(LevelInfo), uint8(LevelWarn), uint8(LevelError), uint8(LevelPanic), uint8(LevelDPanic), uint8(LevelFatal),
	}
}

// highlightKey is a special key that controls highlighting.
const highlightKey = "__highlight"

func compileJQ(p string) (*gojq.Code, error) {
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

// AddJQ compiles the provided jq program and adds it to the filter.
func (f *FilterScheme) AddJQ(p string) error {
	if f.JQ != nil {
		return errors.New("jq program already added")
	}
	jq, err := compileJQ(p)
	if err != nil {
		return err // already has decent annotation
	}
	f.JQ = jq
	return nil
}

// runJQ runs the provided jq program on the provided line.  It returns true if the result is empty
// (i.e., the line should be filtered out), and an error if the output type is invalid or another
// error occurred.
func (f *FilterScheme) runJQ(l *line) (bool, error) {
	if f.JQ == nil {
		return false, nil
	}
	var filtered bool
	iter := f.JQ.Run(l.fields, prepareVariables(l)...)
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

// regexpScope determines what fields a regexp should run against.  Not implemented yet.
type regexpScope int

const (
	regexpScopeUnknown regexpScope = iota
	regexpScopeMessage
)

// runRegexp runs the regexp, returning whether or not it matched.
func runRegexp(rx *regexp.Regexp, l *line, scope regexpScope) bool {
	var input string
	switch scope {
	case regexpScopeUnknown:
		panic("unknown regexp scope")
	case regexpScopeMessage:
		input = l.msg
	}
	fields := rx.FindStringSubmatch(input)
	if len(fields) == 0 {
		return false
	}
	for i, name := range rx.SubexpNames() {
		if i == 0 {
			continue
		}
		if name == "" {
			name = fmt.Sprintf("$%v", i)
		}
		l.fields[name] = fields[i]
	}
	return true
}

// Run runs all the filters defined in this FilterScheme against the provided line.  The return
// value is true if the line should be removed from the output ("filtered").
func (f *FilterScheme) Run(l *line) (bool, error) {
	if rx := f.NoMatchRegex; rx != nil {
		found := runRegexp(rx, l, regexpScopeMessage)
		if found {
			return true, nil
		}
	}
	if rx := f.MatchRegex; rx != nil {
		found := runRegexp(rx, l, regexpScopeMessage)
		if !found {
			return true, nil
		}
	}
	filtered, err := f.runJQ(l)
	if err != nil {
		return false, fmt.Errorf("jq: %w", err)
	}
	return filtered, nil
}
