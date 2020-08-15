package parse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/itchyny/gojq"
)

var (
	basicTimeParser TimeParser = func(t interface{}) (time.Time, error) {
		if x, ok := t.(float64); ok {
			return time.Unix(int64(math.Trunc(x)), int64(x-math.Trunc(x))*1e9), nil
		}
		return time.Unix(-1, 0), errors.New("invalid timestamp")
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
			input: `{"t":1.0,"l":"info","m":"hi"}`,
			want: &line{
				err:    nil,
				time:   time.Unix(1, 0),
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
				time:   time.Unix(1, 0),
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
				time:   time.Unix(2, 0),
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
	panicTime       = time.Unix(666, 0)
	panicMessage    = "panic"
	panicFieldValue = "panic"
)

func (f *testFormatter) FormatTime(s *State, t time.Time, w *bytes.Buffer) error {
	switch t {
	case time.Time{}:
		_, err := w.Write([]byte(fmt.Sprintf("{TS:∅}")))
		return err
	case panicTime:
		panic("panic")
	default:
		_, err := w.Write([]byte(fmt.Sprintf("{TS:%d}", t.Unix())))
		return err
	}
	return nil
}
func (f *testFormatter) FormatLevel(s *State, l Level, w *bytes.Buffer) error {
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
	_, err := w.Write([]byte(fmt.Sprintf("{LVL:%s}", lvl)))
	return err
}
func (f *testFormatter) FormatMessage(s *State, msg string, w *bytes.Buffer) error {
	if msg == panicMessage {
		panic("panic")
	}
	_, err := w.Write([]byte(fmt.Sprintf("{MSG:%s}", msg)))
	return err
}
func (f *testFormatter) FormatField(s *State, k string, v interface{}, w *bytes.Buffer) error {
	if str, ok := v.(string); ok && str == panicFieldValue {
		panic("panic")
	}
	_, err := w.Write([]byte(fmt.Sprintf("{F:%s:%v}", strings.ToUpper(k), v)))
	return err
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
			if err := s.Emit(test.line, w); err != nil {
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

type errReader struct {
	data []byte
	err  error
	i    int
	n    int
}

func (r *errReader) Read(buf []byte) (int, error) {
	var i int
	for ; i+r.i < r.n && i < len(buf); i++ {
		buf[i] = r.data[r.i]
		r.i++
	}
	if r.i == r.n {
		return i, r.err
	}
	return i, nil
}

type errWriter struct {
	bytes.Buffer
	n int
}

func (w *errWriter) Write(buf []byte) (int, error) {
	maxWrite := len(buf)
	if w.n < maxWrite {
		maxWrite = w.n
	}
	w.n -= maxWrite
	var err error
	if w.n <= 0 {
		err = errors.New("broken pipe")
	}
	n, _ := w.Buffer.Write(buf[:maxWrite])
	return n, err
}

var goodLine = "{\"t\":1,\"l\":\"info\",\"m\":\"hi\",\"a\":42}\n"

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
			name:         "valid message",
			r:            strings.NewReader(goodLine + goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n",
			wantSummary:  Summary{Lines: 2},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "valid message with jq program",
			r:            strings.NewReader(goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           mustJQ(".a |= . + $LVL"),
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:45}\n",
			wantSummary:  Summary{Lines: 1},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "broken json",
			r:            strings.NewReader("this is not json\n{\"t\":1,\"m\":\"but this is\",\"l\":\"info\"}\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "this is not json\n{LVL:I} {TS:1} {MSG:but this is}\n",
			wantSummary:  Summary{Lines: 2, Errors: 1},
			wantErrs:     []error{Match("unmarshal json")},
			wantFinalErr: nil,
		},
		{
			name:         "broken json with lax schema",
			r:            strings.NewReader("this is not json\n{\"t\":1,\"m\":\"but this is\",\"l\":\"info\"}\n"),
			w:            new(bytes.Buffer),
			is:           laxSchema,
			wantOutput:   "{LVL:X} {TS:∅} {MSG:this is not json}\n{LVL:I} {TS:1} {MSG:but this is}\n",
			wantSummary:  Summary{Lines: 2},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "read error midway through a line",
			r:            &errReader{data: []byte(goodLine + goodLine), err: errors.New("explosion!"), n: len(goodLine) + 5},
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n" + goodLine[:5] + "\n",
			wantSummary:  Summary{Lines: 2, Errors: 1},
			wantErrs:     []error{Match("unexpected end of JSON input")},
			wantFinalErr: errors.New("explosion!"),
		},
		{
			name:         "write error midway through a line",
			r:            strings.NewReader(goodLine + goodLine),
			w:            &errWriter{n: 43},
			is:           basicSchema,
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n{LVL:I} {T",
			wantSummary:  Summary{Lines: 2, Errors: 1},
			wantErrs:     nil,
			wantFinalErr: Match("broken pipe"),
		},
		{
			name:         "write error midway through a line (2)",
			r:            strings.NewReader(goodLine + goodLine),
			w:            &errWriter{n: 44},
			is:           basicSchema,
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n{LVL:I} {TS",
			wantSummary:  Summary{Lines: 2, Errors: 1},
			wantErrs:     nil,
			wantFinalErr: Match("broken pipe"),
		},
		{
			name:         "filtering out a line with select",
			r:            strings.NewReader(goodLine + goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           mustJQ("select($TS<0)"),
			wantOutput:   "",
			wantSummary:  Summary{Lines: 2, Errors: 0, Filtered: 2},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "removing fields with '{}'",
			r:            strings.NewReader(goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           mustJQ("{}"),
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi}\n",
			wantSummary:  Summary{Lines: 1, Errors: 0, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "panic because of level",
			r:            strings.NewReader(`{"t":1,"l":"panic","m":"m","foo":"bar"}` + "\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   `{"t":1,"l":"panic","m":"m","foo":"bar"}` + "\n",
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("panic"),
		},
		{
			name:         "panic because of time",
			r:            strings.NewReader(`{"t":666,"l":"info","m":"m","foo":"bar"}` + "\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   `{LVL:I} ` + `{"t":666,"l":"info","m":"m","foo":"bar"}` + "\n",
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("panic"),
		},
		{
			name:         "panic because of message",
			r:            strings.NewReader(`{"t":1,"l":"info","m":"panic","foo":"bar"}` + "\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   `{LVL:I} {TS:1} ` + `{"t":1,"l":"info","m":"panic","foo":"bar"}` + "\n",
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("panic"),
		},
		{
			name:         "panic because of field",
			r:            strings.NewReader(`{"t":1,"l":"info","m":"m","a":"first","foo":"panic"}` + "\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   `{LVL:I} {TS:1} {MSG:m} {F:A:first} ` + `{"t":1,"l":"info","m":"m","a":"first","foo":"panic"}` + "\n",
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("panic"),
		},
		// These jq tests are here because I am not sure that I want to make a single jq
		// error abort processing entirely yet.
		{
			name:         "removing fields with 'null'",
			r:            strings.NewReader(goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           mustJQ("null"),
			wantOutput:   goodLine,
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("unexpected nil"),
		},
		{
			name:         "returning an error from jq",
			r:            strings.NewReader(goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           mustJQ("error(\"goodbye\")"),
			wantOutput:   goodLine,
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("goodbye"),
		},
		{
			name:         "returning multiple lines from jq",
			r:            strings.NewReader(goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           mustJQ("{},{}"),
			wantOutput:   goodLine,
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("unexpectedly produced more than 1 output"),
		},
	}
	for _, test := range testData {
		var gotErrs []error
		os := &OutputSchema{
			Formatter:      &testFormatter{},
			EmitErrorFn:    func(x string) { gotErrs = append(gotErrs, errors.New(x)) },
			PriorityFields: []string{"a"},
			state:          State{lastFields: make(map[string][]byte)},
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
			if got, want := err, test.wantFinalErr; !comperror(got, want) {
				t.Errorf("final error:\n  got: %v\n want: %v", got, want)
			}
		})
	}
}
