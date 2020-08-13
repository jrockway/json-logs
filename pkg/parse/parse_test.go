package parse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/itchyny/gojq"
)

var (
	defaultTime                = time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
	basicTimeParser TimeParser = func(t interface{}) (time.Time, error) {
		if x, ok := t.(float64); ok {
			return defaultTime.Add(time.Second * time.Duration(x-1)), nil
		}
		return time.Time{}, errors.New("invalid timestamp")
	}

	basicSchema = &InputSchema{
		LevelKey:    "l",
		MessageKey:  "m",
		TimeKey:     "t",
		TimeFormat:  basicTimeParser,
		LevelFormat: DefaultLevelParser,
		Strict:      true,
	}
	laxSchema = &InputSchema{
		LevelKey:    "l",
		MessageKey:  "m",
		TimeKey:     "t",
		TimeFormat:  basicTimeParser,
		LevelFormat: DefaultLevelParser,
		Strict:      false,
	}
)

type matchingError struct{ re *regexp.Regexp }

func (e *matchingError) Error() string { return "match /" + e.re.String() + "/" }

func Match(x string) error {
	return &matchingError{re: regexp.MustCompile(x)}
}

func comperror(x, y error) bool {
	if x == nil || y == nil {
		return x == y
	}
	if e, ok := y.(*matchingError); ok {
		return e.re.MatchString(x.Error())
	}
	if e, ok := x.(*matchingError); ok {
		return e.re.MatchString(y.Error())
	}
	return x.Error() == y.Error()
}

func mustJQ(prog string) *gojq.Code {
	q, err := gojq.Parse(prog)
	if err != nil {
		panic(err)
	}
	jq, err := gojq.Compile(q, gojq.WithVariables(DefaultVariables))
	if err != nil {
		panic(err)
	}
	return jq
}
func TestRead(t *testing.T) {
	tests := []struct {
		name  string
		s     *InputSchema
		input string
		want  *line
	}{
		{
			name:  "empty message",
			s:     basicSchema,
			input: ``,
			want: &line{
				err: Match("unexpected end of JSON input.*no time key.*no message key.*no level key"),
			},
		},
		{
			name:  "empty message in lax mode",
			s:     laxSchema,
			input: ``,
			want:  &line{},
		},
		{
			name:  "empty json",
			s:     basicSchema,
			input: `{}`,
			want: &line{
				msg: "",
				err: Match("no time key.*no message key.*no level key"),
			},
		},
		{
			name:  "empty json in lax mode",
			s:     laxSchema,
			input: `{}`,
			want: &line{
				msg: "",
			},
		}, {
			name:  "invalid json",
			s:     basicSchema,
			input: `{"not":"json"`,
			want: &line{
				msg: "",
				err: Match("unmarshal json: unexpected end of JSON input"),
			},
		},
		{
			name:  "empty json in lax mode",
			s:     laxSchema,
			input: `{"not":"json"`,
			want: &line{
				msg: `{"not":"json"`,
			},
		},
		{
			name:  "basic successful parse",
			s:     basicSchema,
			input: `{"t":1,"l":"info","m":"hi"}`,
			want: &line{
				err:    nil,
				time:   defaultTime,
				lvl:    LevelInfo,
				msg:    "hi",
				fields: nil,
			},
		},
		{
			name:  "basic successful parse with extra fields",
			s:     basicSchema,
			input: `{"t":1,"l":"info","m":"hi","a":"test"}`,
			want: &line{
				err:    nil,
				time:   defaultTime,
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"a": "test"},
			},
		},
		{
			name:  "basic successful parse with extra fields and lax parser",
			s:     laxSchema,
			input: `{"t":2,"l":"info","m":"hi","a":"test"}`,
			want: &line{
				err:    nil,
				time:   defaultTime.Add(time.Second),
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"a": "test"},
			},
		},
		{
			name:  "missing timestamp",
			s:     basicSchema,
			input: `{"bad_ts":1,"l":"info","m":"hi"}`,
			want: &line{
				err:    Match("no time key"),
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"bad_ts": float64(1)},
			},
		},
		{
			name:  "missing timestamp with lax schema",
			s:     laxSchema,
			input: `{"bad_ts":1,"l":"info","m":"hi"}`,
			want: &line{
				err:    nil,
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"bad_ts": float64(1)},
			},
		},
		{
			name:  "unparseable timestamp",
			s:     basicSchema,
			input: `{"t":"bad","l":"info","m":"hi"}`,
			want: &line{
				err:    Match("invalid timestamp"),
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"t": "bad"},
			},
		},
		{
			name:  "unparseable timestamp with lax schema",
			s:     laxSchema,
			input: `{"t":"bad","l":"info","m":"hi"}`,
			want: &line{
				err:    nil,
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"t": "bad"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := new(line)
			l.fields = make(map[string]interface{})
			l.raw = []byte(test.input)
			test.want.raw = []byte(test.input)
			test.s.ReadLine(l)
			if diff := cmp.Diff(l, test.want, cmp.AllowUnexported(line{}), cmp.Comparer(comperror), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("parsed line differs: %v", diff)
			}
		})
	}
}

type testFormatter struct {
	errors []error
}

var (
	panicTime       = time.Date(2017, 1, 20, 11, 0, 0, 0, time.UTC)
	panicMessage    = "panic"
	panicFieldValue = "panic"
)

func (f *testFormatter) FormatTime(s *State, t time.Time, w io.Writer) error {
	switch t {
	case time.Time{}:
		w.Write([]byte(fmt.Sprintf("{TS:∅}")))
	case panicTime:
		panic("panic")
	default:
		w.Write([]byte(fmt.Sprintf("{TS:%d}", t.Unix())))
	}
	return nil
}
func (f *testFormatter) FormatLevel(s *State, l Level, w io.Writer) error {
	if l == LevelPanic {
		panic("panic")
	}
	lvl := "X"
	switch l {
	case LevelDebug:
		lvl = "D"
	case LevelInfo:
		lvl = "I"
	}
	w.Write([]byte(fmt.Sprintf("{LVL:%s}", lvl)))
	return nil
}
func (f *testFormatter) FormatMessage(s *State, msg string, w io.Writer) error {
	if msg == panicMessage {
		panic("panic")
	}
	w.Write([]byte(fmt.Sprintf("{MSG:%s}", msg)))
	return nil
}
func (f *testFormatter) FormatField(s *State, k string, v interface{}, w io.Writer) error {
	if str, ok := v.(string); ok && str == panicFieldValue {
		panic("panic")
	}
	w.Write([]byte(fmt.Sprintf("{F:%s:%v}", strings.ToUpper(k), v)))
	return nil
}

func TestEmit(t *testing.T) {
	tests := []struct {
		name      string
		state     State
		line      *line
		want      string
		wantErrs  []error
		wantState State
	}{

		{
			name: "empty",
			line: &line{},
			want: "{LVL:X} {TS:∅} {MSG:}",
		},
		{
			name:     "error from previous stage",
			line:     &line{raw: []byte("foo"), err: errors.New("bad")},
			want:     "foo",
			wantErrs: []error{Match("bad")},
		},
		{
			name: "basic",
			line: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				msg:  "hello, world!!",
			},
			want: "{LVL:I} {TS:1} {MSG:hello, world!!}",
		},
		{
			name: "basic with fields",
			line: &line{
				time: time.Unix(2, 0),
				lvl:  LevelDebug,
				msg:  "hi",
				fields: map[string]interface{}{
					"foo": "this is foo",
					"bar": map[string]interface{}{
						"a": 42,
						"b": float64(123),
						"c": []string{"x", "y", "z"},
					},
					"baz": "this is baz",
				},
			},
			state: State{
				seenFields: []string{"bar"},
			},
			want: "{LVL:D} {TS:2} {MSG:hi} {F:BAZ:this is baz} {F:BAR:map[a:42 b:123 c:[x y z]]} {F:FOO:this is foo}",
			wantState: State{
				seenFields: []string{"bar", "foo"},
			},
		},
		{
			name: "panic because of level",
			line: &line{
				time: time.Unix(1, 0),
				lvl:  LevelPanic,
				msg:  "m",
			},
			want:     "",
			wantErrs: []error{Match("^panic")},
		},
		{
			name: "panic because of time",
			line: &line{
				time: panicTime,
				lvl:  LevelUnknown,
				msg:  "m",
			},
			want:     "{LVL:X} ",
			wantErrs: []error{Match("^panic")},
		},
		{
			name: "panic because of message",
			line: &line{
				time: time.Unix(1, 0),
				lvl:  LevelUnknown,
				msg:  panicMessage,
			},
			want:     "{LVL:X} {TS:1} ",
			wantErrs: []error{Match("^panic")},
		},
		{
			name: "panic because of priority field",
			line: &line{
				time:   time.Unix(1, 0),
				lvl:    LevelUnknown,
				msg:    "m",
				fields: map[string]interface{}{"baz": panicFieldValue, "other": "ok"},
			},
			want:     "{LVL:X} {TS:1} {MSG:m} ",
			wantErrs: []error{Match("^panic")},
		},
		{
			name: "panic because of seen field",
			line: &line{
				time:   time.Unix(1, 0),
				lvl:    LevelUnknown,
				msg:    "m",
				fields: map[string]interface{}{"baz": "ok", "other": panicFieldValue, "even_more": "hi"},
			},
			state:     State{seenFields: []string{"other"}},
			wantState: State{seenFields: []string{"other"}},
			want:      "{LVL:X} {TS:1} {MSG:m} {F:BAZ:ok} ",
			wantErrs:  []error{Match("^panic")},
		},
		{
			name: "panic because of new field",
			line: &line{
				time:   time.Unix(1, 0),
				lvl:    LevelUnknown,
				msg:    "m",
				fields: map[string]interface{}{"baz": "ok", "other": "still ok", "even_more": panicFieldValue},
			},
			state:     State{seenFields: []string{"other"}},
			wantState: State{seenFields: []string{"other", "even_more"}},
			want:      "{LVL:X} {TS:1} {MSG:m} {F:BAZ:ok} {F:OTHER:still ok} ",
			wantErrs:  []error{Match("^panic")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := new(bytes.Buffer)
			f := &testFormatter{}
			s := &OutputSchema{
				Formatter:      f,
				EmitErrorFn:    func(x string) { f.errors = append(f.errors, errors.New(x)) },
				PriorityFields: []string{"baz"},
				state:          test.state,
			}
			if err := s.Emit(w, test.line); err != nil {
				f.errors = append(f.errors, err)
			}
			if diff := cmp.Diff(w.String(), test.want+"\n"); diff != "" {
				t.Errorf("emitted output:\n%v", diff)
			}
			if diff := cmp.Diff(f.errors, test.wantErrs, cmp.Comparer(comperror)); diff != "" {
				t.Errorf("errors:\n%v", diff)
			}
			if diff := cmp.Diff(s.state, test.wantState, cmp.AllowUnexported(State{})); diff != "" {
				t.Errorf("state:\n%v", diff)
			}
		})
	}
}

type rw interface {
	String() string
	Write([]byte) (int, error)
}

func TestReadLog(t *testing.T) {
	testData := []struct {
		name         string
		r            io.Reader
		w            rw
		is           *InputSchema
		jq           *gojq.Code
		wantOutput   string
		wantSummary  Summary
		wantErrs     []error
		wantFinalErr error
	}{
		{
			name:         "empty input",
			r:            strings.NewReader("\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "\n",
			wantSummary:  Summary{Lines: 1, Errors: 1},
			wantErrs:     []error{Match("unexpected end of JSON input")},
			wantFinalErr: nil,
		},
		{
			name:         "empty input with lax schema",
			r:            strings.NewReader("\n"),
			w:            new(bytes.Buffer),
			is:           laxSchema,
			wantOutput:   "{LVL:X} {TS:∅} {MSG:}\n",
			wantSummary:  Summary{Lines: 1},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "broken json",
			r:            strings.NewReader("this is not json\n{\"t\":1,\"m\":\"but this is\",\"l\":\"info\"}\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "this is not json\n{LVL:I} {TS:946782245} {MSG:but this is}\n",
			wantSummary:  Summary{Lines: 2, Errors: 1},
			wantErrs:     []error{Match("unmarshal json")},
			wantFinalErr: nil,
		},
		{
			name:         "broken json with lax schema",
			r:            strings.NewReader("this is not json\n{\"t\":1,\"m\":\"but this is\",\"l\":\"info\"}\n"),
			w:            new(bytes.Buffer),
			is:           laxSchema,
			wantOutput:   "{LVL:X} {TS:∅} {MSG:this is not json}\n{LVL:I} {TS:946782245} {MSG:but this is}\n",
			wantSummary:  Summary{Lines: 2},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
	}
	for _, test := range testData {
		var gotErrs []error
		os := &OutputSchema{
			Formatter:   &testFormatter{},
			EmitErrorFn: func(x string) { gotErrs = append(gotErrs, errors.New(x)) },
			state:       State{lastFields: make(map[string][]byte)},
		}

		t.Run(test.name, func(t *testing.T) {
			summary, err := ReadLog(test.r, test.w, test.is, os, test.jq)
			if diff := cmp.Diff(test.w.String(), test.wantOutput); diff != "" {
				t.Errorf("output: %v", diff)
			}
			if diff := cmp.Diff(summary, test.wantSummary); diff != "" {
				t.Errorf("summary: %v", diff)
			}
			if diff := cmp.Diff(gotErrs, test.wantErrs, cmp.Comparer(comperror)); diff != "" {
				t.Errorf("intermediate errors: %v", diff)
			}
			if diff := cmp.Diff(err, test.wantFinalErr, cmp.Comparer(comperror)); diff != "" {
				t.Errorf("final error: %v", diff)
			}
		})
	}
}
