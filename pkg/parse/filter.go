package parse

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/itchyny/gojq"
)

// FilterScheme controls how lines are filtered.
type FilterScheme struct {
	JQ           *gojq.Code
	MatchRegex   *regexp.Regexp
	NoMatchRegex *regexp.Regexp
	Scope        RegexpScope
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

func compileJQ(p string, searchPath []string) (*gojq.Code, error) {
	if p == "" {
		return nil, nil
	}
	q, err := gojq.Parse(p)
	if err != nil {
		return nil, fmt.Errorf("parsing jq program %q: %v", p, err)
	}
	jq, err := gojq.Compile(q,
		gojq.WithFunction("highlight", 1, 1, func(dot interface{}, args []interface{}) interface{} {
			hl, ok := args[0].(bool)
			if !ok {
				return fmt.Errorf("argument to highlight should be a boolean; not %#v", args[0])
			}
			if val, ok := dot.(map[string]interface{}); ok {
				val[highlightKey] = hl
			}
			return dot
		}),
		gojq.WithEnvironLoader(os.Environ),
		gojq.WithVariables(DefaultVariables),
		gojq.WithModuleLoader(gojq.NewModuleLoader(searchPath)))
	if err != nil {
		return nil, fmt.Errorf("compiling jq program %q: %v", p, err)
	}
	return jq, nil
}

type JQOptions struct {
	SearchPath []string
}

// AddJQ compiles the provided jq program and adds it to the filter.
func (f *FilterScheme) AddJQ(p string, opts *JQOptions) error {
	if f.JQ != nil {
		return errors.New("jq program already added")
	}
	var searchPath []string
	if opts != nil {
		searchPath = opts.SearchPath
	}
	jq, err := compileJQ(p, searchPath)
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
	}
	return filtered, nil
}

// RegexpScope determines what fields a regexp should run against.
type RegexpScope int

const (
	RegexpScopeMessage = 1 << iota
	RegexpScopeKeys
	RegexpScopeValues
)

func (s RegexpScope) String() string {
	var parts []string
	if s&RegexpScopeMessage > 0 {
		parts = append(parts, "m")
	}
	if s&RegexpScopeKeys > 0 {
		parts = append(parts, "k")
	}
	if s&RegexpScopeValues > 0 {
		parts = append(parts, "v")
	}
	return strings.Join(parts, "")
}

func (s *RegexpScope) UnmarshalText(text []byte) error {
	for _, b := range text {
		switch b {
		case 'm':
			*s |= RegexpScopeMessage
		case 'k':
			*s |= RegexpScopeKeys
		case 'v':
			*s |= RegexpScopeValues
		default:
			return fmt.Errorf("unrecognized scope character '%c'; [kmv] are recognized", b)
		}
	}
	return nil
}

func (s RegexpScope) MarshalFlag() string              { return s.String() }
func (s *RegexpScope) UnmarshalFlag(text string) error { return s.UnmarshalText([]byte(text)) }

// runRegexp runs the regexp, returning whether or not it matched.
func runRegexp(rx *regexp.Regexp, l *line, scope RegexpScope) bool {
	if scope&RegexpScopeMessage > 0 {
		if applyRegexp(rx, l, l.msg) {
			return true
		}
	}
	if scope&RegexpScopeKeys > 0 {
		for k := range l.fields {
			if applyRegexp(rx, l, k) {
				return true
			}
		}
	}
	if scope&RegexpScopeValues > 0 {
		var addErr error
		for _, v := range l.fields {
			j, err := json.Marshal(v)
			if err != nil {
				// This is very unlikely to happen, but Go code can mutate l.fields
				// to produce something unmarshalable.
				addErr = err
				continue
			}
			if applyRegexp(rx, l, string(j)) {
				return true
			}
		}
		// This is done so that regexps can't match the error message we generate here.
		if addErr != nil {
			l.fields["jlog_match_marshal_error"] = addErr.Error()
		}
	}
	return false
}

// applyRegexp runs the regexp against the provided input, modifying the line in-place and retruning
// whether the regexp matched.
func applyRegexp(rx *regexp.Regexp, l *line, input string) bool {
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
	rxFiltered := false
	if rx := f.NoMatchRegex; rx != nil {
		if found := runRegexp(rx, l, f.Scope); found {
			rxFiltered = true
		}
	}
	if rx := f.MatchRegex; rx != nil {
		if found := runRegexp(rx, l, f.Scope); !found {
			rxFiltered = true
		}
	}
	jqFiltered, err := f.runJQ(l)
	if err != nil {
		return false, fmt.Errorf("jq: %w", err)
	}
	return rxFiltered || jqFiltered, nil
}

var (
	ErrAlreadyAdded = errors.New("regex already added")
	ErrConflict     = errors.New("attempt to add regex when a conflicting regex has already been added")
)

// Add a MatchRegex to this filter scheme.  A MatchRegex filters out all lines that do not match it.
// An empty string is a no-op.  This method may only be called with a non-empty string once, and
// returns an ErrConflict if a NoMatchRegex is set.
func (f *FilterScheme) AddMatchRegex(rx string) error {
	if rx == "" {
		return nil
	}
	if f.MatchRegex != nil {
		return ErrAlreadyAdded
	}
	if f.NoMatchRegex != nil {
		return ErrConflict
	}
	var err error
	f.MatchRegex, err = regexp.Compile(rx)
	if err != nil {
		return fmt.Errorf("compile regex: %w", err)
	}
	return nil
}

// Add a NoMatchRegex to this filter scheme.  A NoMatchRegex filters out all lines that match it.
// An empty string is a no-op.  This method may only be called with a non-empty string once, and
// returns an ErrConflict if a MatchRegex is set.
func (f *FilterScheme) AddNoMatchRegex(rx string) error {
	if rx == "" {
		return nil
	}
	if f.NoMatchRegex != nil {
		return ErrAlreadyAdded
	}
	if f.MatchRegex != nil {
		return ErrConflict
	}
	var err error
	f.NoMatchRegex, err = regexp.Compile(rx)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}
	return nil
}
