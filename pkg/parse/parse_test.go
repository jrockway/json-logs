package parse

import (
	"bytes"
	"encoding/json"
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
	"github.com/logrusorgru/aurora/v3"
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

func modifyBasicSchema(f func(s *InputSchema)) *InputSchema {
	basic := *basicSchema
	f(&basic)
	return &basic
}

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

func TestRead(t *testing.T) {
	tests := []struct {
		name  string
		s     *InputSchema
		input string
		want  *line
		err   error
	}{
		{
			name:  "empty message",
			s:     basicSchema,
			input: ``,
			want:  &line{},
			err:   Match("unexpected end of JSON input.*no time key.*no message key.*no level key"),
		},
		{
			name:  "empty message in lax mode",
			s:     laxSchema,
			input: ``,
			want:  &line{},
			err:   Match("not a JSON object"),
		},
		{
			name:  "empty json",
			s:     basicSchema,
			input: `{}`,
			want: &line{
				msg: "",
			},
			err: Match("no time key.*no message key.*no level key"),
		},
		{
			name:  "empty json in lax mode",
			s:     laxSchema,
			input: `{}`,
			want: &line{
				msg: "",
			},
			err: Match("no time key.*no message key.*no level key"),
		}, {
			name:  "invalid json",
			s:     basicSchema,
			input: `{"not":"json"`,
			want: &line{
				msg: "",
			},
			err: Match("unmarshal json: unexpected end of JSON input"),
		},
		{
			name:  "empty json in lax mode",
			s:     laxSchema,
			input: `{"not":"json"`,
			want: &line{
				msg: `{"not":"json"`,
			},
			err: Match("unmarshal json: unexpected end of JSON input"),
		},
		{
			name:  "basic successful parse",
			s:     basicSchema,
			input: `{"t":1.0,"l":"info","m":"hi"}`,
			want: &line{
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
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"bad_ts": float64(1)},
			},
			err: Match("no time key"),
		},
		{
			name:  "missing timestamp with lax schema",
			s:     laxSchema,
			input: `{"bad_ts":1,"l":"info","m":"hi"}`,
			want: &line{
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"bad_ts": float64(1)},
			},
			err: Match("no time key"),
		},
		{
			name:  "unparseable timestamp",
			s:     basicSchema,
			input: `{"t":"bad","l":"info","m":"hi"}`,
			want: &line{
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"t": "bad"},
			},
			err: Match("invalid timestamp"),
		},
		{
			name:  "unparseable timestamp with lax schema",
			s:     laxSchema,
			input: `{"t":"bad","l":"info","m":"hi"}`,
			want: &line{
				lvl:    LevelInfo,
				msg:    "hi",
				fields: map[string]interface{}{"t": "bad"},
			},
			err: Match("invalid timestamp"),
		},
		{
			name:  "non-string message",
			s:     basicSchema,
			input: `{"t":1,"l":"info","m":42,"a":123}`,
			want: &line{
				time:   time.Unix(1, 0),
				lvl:    LevelInfo,
				msg:    `{"t":1,"l":"info","m":42,"a":123}`,
				fields: map[string]interface{}{"a": float64(123), "m": float64(42)},
			},
			err: Match("non-string data"),
		},
		{
			name:  "non-string level",
			s:     basicSchema,
			input: `{"t":1,"l":42,"m":"hi","a":123}`,
			want: &line{
				time:   time.Unix(1, 0),
				lvl:    LevelUnknown,
				msg:    `hi`,
				fields: map[string]interface{}{"a": float64(123), "l": float64(42)},
			},
			err: Match("invalid float64\\(42\\) for log level"),
		},
		{
			name:  "valid upgrade",
			s:     modifyBasicSchema(func(s *InputSchema) { s.UpgradeKeys = []string{"upgrade"} }),
			input: `{"t":1,"l":"info","m":"test","existed":true,"overwritten":"nope","upgrade":{"key":"value","num":42.1,"overwritten":true}}`,
			want: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				msg:  "test",
				fields: map[string]interface{}{
					"existed":     true,
					"key":         "value",
					"num":         float64(42.1),
					"overwritten": true,
				},
			},
		},
		{
			name:  "valid upgrade, overwrite self",
			s:     modifyBasicSchema(func(s *InputSchema) { s.UpgradeKeys = []string{"upgrade"} }),
			input: `{"t":1,"l":"info","m":"test","existed":true,"upgrade":{"upgrade":"hello"}}`,
			want: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				msg:  "test",
				fields: map[string]interface{}{
					"existed": true,
					"upgrade": "hello",
				},
			},
		},
		{
			name:  "valid upgrade, but nothing to upgrade",
			s:     modifyBasicSchema(func(s *InputSchema) { s.UpgradeKeys = []string{"upgrade"} }),
			input: `{"t":1,"l":"info","m":"test","existed":true,"overwritten":"nope"}`,
			want: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				msg:  "test",
				fields: map[string]interface{}{
					"existed":     true,
					"overwritten": "nope",
				},
			},
		},
		{
			name:  "invalid upgrade",
			s:     modifyBasicSchema(func(s *InputSchema) { s.UpgradeKeys = []string{"upgrade"} }),
			input: `{"t":1,"l":"info","m":"test","existed":true,"overwritten":"nope","upgrade":["foo"]}`,
			want: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				msg:  "test",
				fields: map[string]interface{}{
					"existed":     true,
					"overwritten": "nope",
					"upgrade":     []interface{}{"foo"},
				},
			},
			err: Match(`upgrade key "upgrade": invalid data type.*got \[\]`),
		},
		{
			name:  "invalid upgrade, lax",
			s:     modifyBasicSchema(func(s *InputSchema) { s.Strict = false; s.UpgradeKeys = []string{"upgrade"} }),
			input: `{"t":1,"l":"info","m":"test","existed":true,"overwritten":"nope","upgrade":["foo"]}`,
			want: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				msg:  "test",
				fields: map[string]interface{}{
					"existed":     true,
					"overwritten": "nope",
					"upgrade":     []interface{}{"foo"},
				},
			},
		},
		{
			name:  "log without time",
			s:     modifyBasicSchema(func(s *InputSchema) { s.TimeKey = ""; s.NoTimeKey = true; s.TimeFormat = NoopTimeParser }),
			input: `{"l":"info","m":"test"}`,
			want: &line{
				lvl: LevelInfo,
				msg: "test",
			},
		},
		{
			name:  "log without time, but time supplied",
			s:     modifyBasicSchema(func(s *InputSchema) { s.NoTimeKey = true; s.TimeFormat = NoopTimeParser }),
			input: `{"t":1,"l":"info","m":"test"}`,
			want: &line{
				lvl: LevelInfo,
				msg: "test",
				fields: map[string]interface{}{
					"t": float64(1),
				},
			},
		},
		{
			name:  "log without level",
			s:     modifyBasicSchema(func(s *InputSchema) { s.LevelKey = ""; s.NoLevelKey = true; s.LevelFormat = NoopLevelParser }),
			input: `{"t":1,"m":"test"}`,
			want: &line{
				time: time.Unix(1, 0),
				msg:  "test",
			},
		},
		{
			name:  "log without level, but level supplied",
			s:     modifyBasicSchema(func(s *InputSchema) { s.NoLevelKey = true; s.LevelFormat = NoopLevelParser }),
			input: `{"t":1,"l":"info","m":"test"}`,
			want: &line{
				time: time.Unix(1, 0),
				msg:  "test",
				fields: map[string]interface{}{
					"l": "info",
				},
			},
		},
		{
			name:  "log without message",
			s:     modifyBasicSchema(func(s *InputSchema) { s.MessageKey = ""; s.NoMessageKey = true }),
			input: `{"t":1,"l":"info"}`,
			want: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
			},
		},
		{
			name:  "log without message, but message supplied",
			s:     modifyBasicSchema(func(s *InputSchema) { s.NoMessageKey = true }),
			input: `{"t":1,"l":"info","m":"test"}`,
			want: &line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				fields: map[string]interface{}{
					"m": "test",
				},
			},
		},

		// Auto-guess tests
		{
			name:  "auto-guess zap",
			s:     &InputSchema{Strict: true},
			input: `{"ts":1,"msg":"hi","level":"info","extra":"is here"}`,
			want: &line{
				time:   time.Unix(1, 0),
				lvl:    LevelInfo,
				msg:    `hi`,
				fields: map[string]interface{}{"extra": "is here"},
			},
			err: nil,
		},
		{
			name:  "auto-guess zap (lax)",
			s:     &InputSchema{Strict: false},
			input: `{"ts":1,"msg":"hi","level":"info","extra":"is here"}`,
			want: &line{
				time:   time.Unix(1, 0),
				lvl:    LevelInfo,
				msg:    `hi`,
				fields: map[string]interface{}{"extra": "is here"},
			},
			err: nil,
		},
		{
			name:  "auto-guess stackdriver",
			s:     &InputSchema{Strict: true},
			input: `{"timestamp":{"seconds":1,"nanos":1},"message":"hi","severity":"info","extra":"is here"}`,
			want: &line{
				time:   time.Unix(1, 1),
				lvl:    LevelInfo,
				msg:    `hi`,
				fields: map[string]interface{}{"extra": "is here"},
			},
			err: nil,
		},
		{
			name:  "auto-guess logrus",
			s:     &InputSchema{Strict: true},
			input: `{"time":"1970-01-01T00:00:01.001Z","msg":"hi","level":"info","extra":"is here"}`,
			want: &line{
				time:   time.Unix(1, 1e6),
				lvl:    LevelInfo,
				msg:    `hi`,
				fields: map[string]interface{}{"extra": "is here"},
			},
			err: nil,
		},
		{
			name:  "auto-guess lager (pretty)",
			s:     &InputSchema{Strict: true},
			input: `{"timestamp":"1970-01-01T00:00:01.001Z","message":"hi","level":"info","source":"test","data":{"extra":"is here"}}`,
			want: &line{
				time:   time.Unix(1, 1e6),
				lvl:    LevelInfo,
				msg:    `hi`,
				fields: map[string]interface{}{"source": "test", "extra": "is here"},
			},
			err: nil,
		},
		{
			name:  "auto-guess lager",
			s:     &InputSchema{Strict: true},
			input: `{"timestamp":1.1,"message":"hi","log_level":1,"source":"test","data":{"extra":"is here"}}`,
			want: &line{
				time:   time.Unix(1, 1e8),
				lvl:    LevelInfo,
				msg:    `hi`,
				fields: map[string]interface{}{"source": "test", "extra": "is here"},
			},
			err: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := new(line)
			l.fields = make(map[string]interface{})
			l.raw = []byte(test.input)
			test.want.raw = []byte(test.input)
			err := test.s.ReadLine(l)
			if diff := cmp.Diff(l, test.want, cmp.AllowUnexported(line{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("parsed line differs: %v", diff)
			}
			if !comperror(err, test.err) {
				t.Errorf("error:\n  got: %v\n want: %v", err, test.err)
			}
		})
	}
}

type testFormatter struct{}

var (
	panicTime       = time.Unix(666, 0)
	panicMessage    = "panic"
	panicFieldValue = "panic"
)

func (f *testFormatter) FormatTime(s *State, t time.Time, w *bytes.Buffer) {
	switch t {
	case panicTime:
		panic("panic")
	case time.Time{}:
		w.WriteString("{TS:∅}")
	default:
		fmt.Fprintf(w, "{TS:%d}", t.Unix())
	}
}
func (f *testFormatter) FormatLevel(s *State, l Level, w *bytes.Buffer) {
	if l == LevelPanic {
		panic("panic")
	}
	var lvl string
	switch l {
	case LevelDebug:
		lvl = "D"
	case LevelInfo:
		lvl = "I"
	case LevelWarn:
		lvl = "W"
	default:
		lvl = "X"
	}
	fmt.Fprintf(w, "{LVL:%s}", lvl)
}
func (f *testFormatter) FormatMessage(s *State, msg string, highlight bool, w *bytes.Buffer) {
	if msg == panicMessage {
		panic("panic")
	}
	if highlight {
		msg = "[" + msg + "]"
	}
	fmt.Fprintf(w, "{MSG:%s}", msg)
}
func (f *testFormatter) FormatField(s *State, k string, v interface{}, w *bytes.Buffer) {
	if str, ok := v.(string); ok {
		if str == panicFieldValue {
			panic("panic")
		}
	}
	value := []byte(fmt.Sprintf("%v", v))
	if s.lastFields != nil {
		old, ok := s.lastFields[k]
		if ok && bytes.Equal(old, value) {
			value = []byte("<same>")
		} else {
			s.lastFields[k] = value
		}
	}
	fmt.Fprintf(w, "{F:%s:%s}", strings.ToUpper(k), value)
}

func TestEmit(t *testing.T) {
	tests := []struct {
		name      string
		state     State
		line      line
		want      string
		wantState State
	}{

		{
			name: "empty",
			line: line{},
			want: "{LVL:X} {TS:∅} {MSG:}\n",
		},
		{
			name: "basic",
			line: line{
				time: time.Unix(1, 0),
				lvl:  LevelInfo,
				msg:  "hello, world!!",
			},
			want: "{LVL:I} {TS:1} {MSG:hello, world!!}\n",
		},
		{
			name: "basic with fields",
			line: line{
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
			state:     State{seenFields: []string{"bar"}},
			want:      "{LVL:D} {TS:2} {MSG:hi} {F:BAZ:this is baz} {F:BAR:map[a:42 b:123 c:[x y z]]} {F:FOO:this is foo}\n",
			wantState: State{seenFields: []string{"bar", "foo"}},
		},
		{
			name: "basic with remembered fields",
			line: line{
				time: time.Unix(3, 0),
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
				seenFields: []string{"foo", "bar"},
				lastFields: map[string][]byte{"foo": []byte("this is foo")},
			},
			want: "{LVL:D} {TS:3} {MSG:hi} {F:BAZ:this is baz} {F:FOO:<same>} {F:BAR:map[a:42 b:123 c:[x y z]]}\n",
			wantState: State{
				lastFields: map[string][]byte{
					"foo": []byte("this is foo"),
					"bar": []byte("map[a:42 b:123 c:[x y z]]"),
					"baz": []byte("this is baz"),
				},
				seenFields: []string{"foo", "bar"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := new(bytes.Buffer)
			f := &testFormatter{}
			s := &OutputSchema{
				Formatter:      f,
				EmitErrorFn:    func(x string) { panic("unused") },
				PriorityFields: []string{"baz"},
				state:          test.state,
			}
			s.Emit(test.line, w)
			if diff := cmp.Diff(w.String(), test.want); diff != "" {
				t.Errorf("emitted output:\n%v", diff)
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

var goodLine = `{"t":1,"l":"info","m":"hi","a":42}` + "\n"

func TestReadLog(t *testing.T) {
	testData := []struct {
		name                   string
		r                      io.Reader
		w                      rw
		is                     *InputSchema
		jq, matchrx, nomatchrx string
		wantOutput             string
		wantSummary            Summary
		wantErrs               []error
		wantFinalErr           error
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
			wantSummary:  Summary{Lines: 1, Errors: 1},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "valid message",
			r:            strings.NewReader(goodLine + goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n{LVL:I} {TS:1} {MSG:hi} {F:A:<same>}\n",
			wantSummary:  Summary{Lines: 2},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "eliding fields",
			r:            strings.NewReader(`{"t":1,"l":"x","m":"m","a":1}` + "\n" + `{"t":2,"l":"x","m":"m","b":2}` + "\n" + `{"t":3,"l":"x","m":"m","a":1,"b":2}` + "\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "{LVL:X} {TS:1} {MSG:m} {F:A:1}\n{LVL:X} {TS:2} {MSG:m} {F:B:2}\n{LVL:X} {TS:3} {MSG:m} {F:A:1} {F:B:<same>}\n",
			wantSummary:  Summary{Lines: 3},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "valid message with jq program",
			r:            strings.NewReader(goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           ".a |= . + $LVL",
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
			wantSummary:  Summary{Lines: 2, Errors: 1},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "invalid line, but it's filtered by a jq program",
			r:            strings.NewReader(`{"a":42}` + "\n"),
			w:            new(bytes.Buffer),
			is:           laxSchema,
			jq:           "select(.a!=42)",
			wantOutput:   "",
			wantSummary:  Summary{Lines: 1, Filtered: 1, Errors: 1},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name:         "invalid line, but it's filtered by a jq program (strict)",
			r:            strings.NewReader(`{"a":42}` + "\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           "select(.a!=42)",
			wantOutput:   `{"a":42}` + "\n",
			wantSummary:  Summary{Lines: 1, Errors: 1},
			wantErrs:     []error{Match("no time key")},
			wantFinalErr: nil,
		},
		{
			name:         "read error midway through a line",
			r:            &errReader{data: []byte(goodLine + goodLine), err: errors.New("explosion"), n: len(goodLine) + 5},
			w:            new(bytes.Buffer),
			is:           basicSchema,
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n" + goodLine[:5] + "\n",
			wantSummary:  Summary{Lines: 2, Errors: 1},
			wantErrs:     []error{Match("unexpected end of JSON input")},
			wantFinalErr: errors.New("explosion"),
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
			name:         "write error midway through writing a raw log",
			r:            strings.NewReader(`this is a bad line`),
			w:            &errWriter{n: 1},
			is:           basicSchema,
			wantOutput:   "t",
			wantSummary:  Summary{Lines: 1, Errors: 1},
			wantErrs:     nil,
			wantFinalErr: Match("broken pipe"),
		},
		{
			name:         "write error midway through emitting an error",
			r:            strings.NewReader(`{"a":42}`),
			w:            &errWriter{n: 23},
			is:           laxSchema,
			wantOutput:   "{LVL:X} {TS:∅} {MSG:}",
			wantSummary:  Summary{Lines: 1, Errors: 1},
			wantErrs:     nil,
			wantFinalErr: Match("broken pipe.*while flushing buffer after error"),
		},
		{
			name:         "filtering out a line with select",
			r:            strings.NewReader(goodLine + goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           "select($TS<0)",
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
			jq:           "{}",
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
		{
			name: "log without time, level, and message",
			r:    strings.NewReader(`{"t":1,"l":"info","m":"hi","a":"value"}` + "\n" + `{"a":"value","t":1}` + "\n{}\n"),
			w:    new(bytes.Buffer),
			is: modifyBasicSchema(func(s *InputSchema) {
				s.TimeFormat = NoopTimeParser
				s.LevelFormat = NoopLevelParser
				s.NoTimeKey = true
				s.NoLevelKey = true
				s.NoMessageKey = true
			}),
			wantSummary:  Summary{Lines: 3, Errors: 0},
			wantOutput:   "{F:A:value} {F:T:1} {F:L:info} {F:M:hi}\n{F:A:<same>} {F:T:<same>}\n\n",
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		{
			name: "log without level",
			r:    strings.NewReader(`{"t":1,"l":"info","m":"line 1","a":"value"}` + "\n" + `{"a":"value","t":2,"m":"line 2"}` + "\n" + `{"t":3,"m":"line 3"}` + "\n"),
			w:    new(bytes.Buffer),
			is: modifyBasicSchema(func(s *InputSchema) {
				s.LevelFormat = NoopLevelParser
				s.NoLevelKey = true
			}),
			wantSummary:  Summary{Lines: 3, Errors: 0},
			wantOutput:   "{TS:1} {MSG:line 1} {F:A:value} {F:L:info}\n{TS:2} {MSG:line 2} {F:A:<same>}\n{TS:3} {MSG:line 3}\n",
			wantErrs:     nil,
			wantFinalErr: nil,
		},
		// These jq tests are here because I am not sure that I want to make a single jq
		// error abort processing entirely yet.
		{
			name:         "removing fields with 'null'",
			r:            strings.NewReader(goodLine),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           "null",
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
			jq:           `error("goodbye")`,
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
			jq:           "{},{}",
			wantOutput:   goodLine,
			wantSummary:  Summary{Lines: 1, Errors: 1, Filtered: 0},
			wantErrs:     nil,
			wantFinalErr: Match("unexpectedly produced more than 1 output"),
		},
		{
			name:         "highlighting messages",
			r:            strings.NewReader(`{"t":1,"l":"info","m":"hi","a":42}` + "\n" + `{"t":1,"l":"warn","m":"hi","a":42}` + "\n"),
			w:            new(bytes.Buffer),
			is:           basicSchema,
			jq:           `highlight($LVL==$WARN)`,
			wantOutput:   "{LVL:I} {TS:1} {MSG:hi} {F:A:42}\n{LVL:W} {TS:1} {MSG:[hi]} {F:A:<same>}\n",
			wantSummary:  Summary{Lines: 2},
			wantErrs:     nil,
			wantFinalErr: nil,
		},
	}
	for _, test := range testData {
		var gotErrs []error
		os := &OutputSchema{
			Formatter:      &testFormatter{},
			EmitErrorFn:    func(x string) { gotErrs = append(gotErrs, errors.New(x)) },
			PriorityFields: []string{"a", "t", "l", "m"},
			state:          State{lastFields: make(map[string][]byte)},
		}

		t.Run(test.name, func(t *testing.T) {
			fs := new(FilterScheme)
			if err := fs.AddJQ(test.jq); err != nil {
				t.Fatalf("add jq: %v", err)
			}
			if err := fs.AddMatchRegex(test.matchrx); err != nil {
				t.Fatalf("add matchregex: %v", err)
			}
			if err := fs.AddNoMatchRegex(test.nomatchrx); err != nil {
				t.Fatalf("add nomatchregex: %v", err)
			}
			summary, err := ReadLog(test.r, test.w, test.is, os, fs)
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

func TestReadLogWithNullFormatter(t *testing.T) {
	r := strings.NewReader(`{"level":"info","ts":12345,"msg":"foo"}` + "\n")
	w := io.Discard
	is := &InputSchema{Strict: false}
	os := &OutputSchema{}
	fs := new(FilterScheme)
	if _, err := ReadLog(r, w, is, os, fs); err != nil {
		t.Fatal(err)
	}
}

func ts(t float64) time.Time {
	fl := math.Floor(t)
	sec := int64(fl)
	nsec := int64(1e3 * math.Round(1e6*(t-fl)))
	return time.Unix(sec, nsec)
}

func makeLog(t time.Time, lvl, message string, fields map[string]any) string {
	x := map[string]any{
		"ts":    t.In(time.UTC).Format(time.RFC3339Nano),
		"level": lvl,
		"msg":   message,
	}
	for k, v := range fields {
		x[k] = v
	}
	b, err := json.Marshal(x)
	if err != nil {
		panic(fmt.Sprintf("make log: marshal: %v", err))
	}
	return string(b)
}

var (
	testLog = []string{
		makeLog(ts(1), "info", "start", nil),
		makeLog(ts(1.000001), "debug", "reading config", map[string]any{"file": "/tmp/config.json"}),
		makeLog(ts(1.000002), "debug", "reading config", map[string]any{"file": "/tmp/config-overlay.json"}),
		makeLog(ts(1.002), "info", "serving", map[string]any{"port": 8080}),
		makeLog(ts(10), "debug", "started incoming request", map[string]any{"route": "/example", "request_id": 1234}),
		makeLog(ts(10.01), "debug", "started incoming request", map[string]any{"route": "/test", "request_id": 4321}),
		makeLog(ts(10.02), "debug", "finished incoming request", map[string]any{"route": "/example", "request_id": 1234, "response_code": 200}),
		makeLog(ts(10.0201), "warn", "user not found", map[string]any{"route": "/test", "request_id": 4321}),
		makeLog(ts(10.0202), "error", "finished incoming request", map[string]any{"route": "/test", "request_id": 4321, "response_code": 401}),
		makeLog(ts(10.03), "debug", "started incoming request", map[string]any{"route": "/example", "request_id": 5432}),
		makeLog(ts(10.031), "debug", "finished incoming request", map[string]any{"route": "/example", "request_id": 5432, "response_code": 200}),
		makeLog(ts(100), "info", "shutting down server; waiting for connections to drain", map[string]any{"port": 8080}),
		makeLog(ts(115), "info", "connections drained", map[string]any{"port": 8080}),
	}

	formattedTestLog = []string{
		"INFO  Jan  1 00:00:01.000000 start",
		"DEBUG                .000001 reading config file:/tmp/config.json",
		"DEBUG                .000002 reading config file:/tmp/config-overlay.json",
		"INFO                 .002000 serving port:8080",
		"DEBUG Jan  1 00:00:10.000000 started incoming request request_id:1234 route:/example",
		"DEBUG                .010000 started incoming request request_id:4321 route:/test",
		"DEBUG                .020000 finished incoming request request_id:1234 route:/example response_code:200",
		"WARN                 .020100 user not found request_id:4321 route:/test",
		"ERROR                .020200 finished incoming request request_id:↑ route:↑ response_code:401",
		"DEBUG                .030000 started incoming request request_id:5432 route:/example",
		"DEBUG                .031000 finished incoming request request_id:↑ route:↑ response_code:200",
		"INFO  Jan  1 00:01:40.000000 shutting down server; waiting for connections to drain port:8080",
		"INFO  Jan  1 00:01:55.000000 connections drained port:↑",
	}
)

func TestFullLog(t *testing.T) {
	testData := []struct {
		name                         string
		jq, matchregex, nomatchregex string
		beforecontext, aftercontext  int
		input                        []string
		wantOutput                   []string
	}{
		{
			name:       "no filtering",
			input:      testLog,
			wantOutput: formattedTestLog,
		},
		{
			name:          "no filtering, before context",
			input:         testLog,
			beforecontext: 2,
			wantOutput:    formattedTestLog,
		},
		{
			name:         "no filtering, after context",
			input:        testLog,
			aftercontext: 2,
			wantOutput:   formattedTestLog,
		},
		{
			name:          "no filtering, context",
			input:         testLog,
			beforecontext: 2,
			aftercontext:  2,
			wantOutput:    formattedTestLog,
		},
		{
			name:  "jq filter by request id",
			input: testLog,
			jq:    `select(.request_id == 1234)`,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:10.000000 started incoming request request_id:1234 route:/example",
				"DEBUG                .020000 finished incoming request request_id:↑ route:↑ response_code:200",
			},
		},
		{
			name:          "jq filter by request id, before context",
			input:         testLog,
			jq:            `select(.request_id == 1234)`,
			beforecontext: 2,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:01.000002 reading config file:/tmp/config-overlay.json",
				"INFO                 .002000 serving port:8080",
				"DEBUG Jan  1 00:00:10.000000 started incoming request request_id:1234 route:/example",
				"DEBUG                .010000 started incoming request request_id:4321 route:/test",
				"DEBUG                .020000 finished incoming request request_id:1234 route:/example response_code:200",
			},
		},
		{
			name:         "jq filter by request id, after context",
			input:        testLog,
			jq:           `select(.request_id == 1234)`,
			aftercontext: 2,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:10.000000 started incoming request request_id:1234 route:/example",
				"DEBUG                .010000 started incoming request request_id:4321 route:/test",
				"DEBUG                .020000 finished incoming request request_id:1234 route:/example response_code:200",
				"WARN                 .020100 user not found request_id:4321 route:/test",
				"ERROR                .020200 finished incoming request request_id:↑ route:↑ response_code:401",
			},
		},
		{
			name:          "jq filter by request id, both context",
			input:         testLog,
			jq:            `select(.request_id == 1234)`,
			aftercontext:  2,
			beforecontext: 2,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:01.000002 reading config file:/tmp/config-overlay.json",
				"INFO                 .002000 serving port:8080",
				"DEBUG Jan  1 00:00:10.000000 started incoming request request_id:1234 route:/example",
				"DEBUG                .010000 started incoming request request_id:4321 route:/test",
				"DEBUG                .020000 finished incoming request request_id:1234 route:/example response_code:200",
				"WARN                 .020100 user not found request_id:4321 route:/test",
				"ERROR                .020200 finished incoming request request_id:↑ route:↑ response_code:401",
			},
		},
		{
			name:          "jq filter, non-contiguous",
			input:         testLog,
			jq:            `select(.port == 8080)`,
			aftercontext:  1,
			beforecontext: 1,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:01.000002 reading config file:/tmp/config-overlay.json",
				"INFO                 .002000 serving port:8080",
				"DEBUG Jan  1 00:00:10.000000 started incoming request request_id:1234 route:/example",
				"---",
				"DEBUG                .031000 finished incoming request request_id:5432 route:↑ response_code:200",
				"INFO  Jan  1 00:01:40.000000 shutting down server; waiting for connections to drain port:8080",
				"INFO  Jan  1 00:01:55.000000 connections drained port:↑",
			},
		},
		{
			name:       "regex match",
			input:      testLog,
			matchregex: `(?P<state>started|finished) (?P<dir>incoming|outgoing)`,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:10.000000 started incoming request dir:incoming request_id:1234 route:/example state:started",
				"DEBUG                .010000 started incoming request dir:↑ request_id:4321 route:/test state:↑",
				"DEBUG                .020000 finished incoming request dir:↑ request_id:1234 route:/example state:finished response_code:200",
				"ERROR                .020200 finished incoming request dir:↑ request_id:4321 route:/test state:↑ response_code:401",
				"DEBUG                .030000 started incoming request dir:↑ request_id:5432 route:/example state:started",
				"DEBUG                .031000 finished incoming request dir:↑ request_id:↑ route:↑ state:finished response_code:200",
			},
		},
		{
			name:          "regex match, before context",
			input:         testLog,
			beforecontext: 2,
			matchregex:    `(?P<state>started|finished) (?P<dir>incoming|outgoing)`,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:01.000002 reading config file:/tmp/config-overlay.json",
				"INFO                 .002000 serving port:8080",
				"DEBUG Jan  1 00:00:10.000000 started incoming request dir:incoming request_id:1234 route:/example state:started",
				"DEBUG                .010000 started incoming request dir:↑ request_id:4321 route:/test state:↑",
				"DEBUG                .020000 finished incoming request dir:↑ request_id:1234 route:/example state:finished response_code:200",
				"WARN                 .020100 user not found request_id:4321 route:/test",
				"ERROR                .020200 finished incoming request dir:incoming request_id:↑ route:↑ state:finished response_code:401",
				"DEBUG                .030000 started incoming request dir:↑ request_id:5432 route:/example state:started",
				"DEBUG                .031000 finished incoming request dir:↑ request_id:↑ route:↑ state:finished response_code:200",
			},
		},
		{
			name:       "regex match with jq",
			input:      testLog,
			matchregex: `(?P<state>started|finished) (?P<dir>incoming|outgoing)`,
			jq:         `select(.state == "started") | {state, dir}`,
			wantOutput: []string{
				"DEBUG Jan  1 00:00:10.000000 started incoming request dir:incoming state:started",
				"DEBUG                .010000 started incoming request dir:↑ state:↑",
				"DEBUG                .030000 started incoming request dir:↑ state:↑",
			},
		},
		{
			name:          "regex match with jq, with context",
			input:         testLog,
			matchregex:    `(?P<state>started|finished) (?P<dir>incoming|outgoing)`,
			jq:            `select(.state == "started") | {}`,
			beforecontext: 1,
			aftercontext:  1,
			wantOutput: []string{
				"INFO  Jan  1 00:00:01.002000 serving port:8080",
				"DEBUG Jan  1 00:00:10.000000 started incoming request",
				"DEBUG                .010000 started incoming request",
				"DEBUG                .020000 finished incoming request dir:incoming request_id:1234 response_code:200 route:/example state:finished",
				"---",
				"ERROR                .020200 finished incoming request dir:↑ request_id:4321 response_code:401 route:/test state:↑",
				"DEBUG                .030000 started incoming request",
				"DEBUG                .031000 finished incoming request dir:incoming request_id:5432 response_code:200 route:/example state:finished",
			},
		},
		{
			name:          "regex nomatch",
			input:         testLog,
			beforecontext: 1,
			aftercontext:  1,
			nomatchregex:  `(started|finished) incoming request`,
			wantOutput: []string{
				"INFO  Jan  1 00:00:01.000000 start",
				"DEBUG                .000001 reading config file:/tmp/config.json",
				"DEBUG                .000002 reading config file:/tmp/config-overlay.json",
				"INFO                 .002000 serving port:8080",
				"DEBUG Jan  1 00:00:10.000000 started incoming request $1:started request_id:1234 route:/example",
				"---",
				"DEBUG                .020000 finished incoming request $1:finished request_id:↑ route:↑ response_code:200",
				"WARN                 .020100 user not found request_id:4321 route:/test",
				"ERROR                .020200 finished incoming request $1:finished request_id:↑ route:↑ response_code:401",
				"---",
				"DEBUG                .031000 finished incoming request $1:↑ request_id:5432 route:/example response_code:200",
				"INFO  Jan  1 00:01:40.000000 shutting down server; waiting for connections to drain port:8080",
				"INFO  Jan  1 00:01:55.000000 connections drained port:↑",
			},
		},
		{
			name:          "regex nomatch with jq",
			input:         testLog,
			beforecontext: 1,
			aftercontext:  1,
			nomatchregex:  `(started|finished) incoming request`,
			jq:            `if ."$1" != null then {"$1"} else {} end`,
			wantOutput: []string{
				"INFO  Jan  1 00:00:01.000000 start",
				"DEBUG                .000001 reading config",
				"DEBUG                .000002 reading config",
				"INFO                 .002000 serving",
				"DEBUG Jan  1 00:00:10.000000 started incoming request $1:started",
				"---",
				"DEBUG                .020000 finished incoming request $1:finished",
				"WARN                 .020100 user not found",
				"ERROR                .020200 finished incoming request $1:finished",
				"---",
				"DEBUG                .031000 finished incoming request $1:↑",
				"INFO  Jan  1 00:01:40.000000 shutting down server; waiting for connections to drain",
				"INFO  Jan  1 00:01:55.000000 connections drained",
			},
		},
		{
			name:          "no output, regex",
			input:         testLog,
			beforecontext: 1,
			aftercontext:  1,
			matchregex:    "this matches nothing",
		},
		{
			name:          "no output, regex nomatch",
			input:         testLog,
			beforecontext: 1,
			aftercontext:  1,
			nomatchregex:  ".*",
		},
		{
			name:          "jq",
			input:         testLog,
			beforecontext: 1,
			aftercontext:  1,
			jq:            "empty",
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			fs := new(FilterScheme)
			if err := fs.AddJQ(test.jq); err != nil {
				t.Fatal(err)
			}
			if err := fs.AddMatchRegex(test.matchregex); err != nil {
				t.Fatal(err)
			}
			if err := fs.AddNoMatchRegex(test.nomatchregex); err != nil {
				t.Fatal(err)
			}

			r := strings.NewReader(strings.Join(test.input, "\n"))
			w := new(bytes.Buffer)

			is := &InputSchema{
				TimeKey:     "ts",
				TimeFormat:  DefaultTimeParser,
				LevelKey:    "level",
				LevelFormat: DefaultLevelParser,
				MessageKey:  "msg",
				Strict:      true,
			}

			os := &OutputSchema{
				Formatter: &DefaultOutputFormatter{
					Aurora:               aurora.NewAurora(false),
					ElideDuplicateFields: true,
					AbsoluteTimeFormat:   time.StampMicro,
					SubSecondsOnlyFormat: "               .000000",
					Zone:                 time.UTC,
				},
				BeforeContext: test.beforecontext,
				AfterContext:  test.aftercontext,
			}

			if _, err := ReadLog(r, w, is, os, fs); err != nil {
				t.Errorf("read log: unexpected error: %v", err)
			}

			out := strings.Split(w.String(), "\n")
			if len(out) > 0 && out[len(out)-1] == "" {
				out = out[:len(out)-1]
			}
			if diff := cmp.Diff(out, test.wantOutput, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("output:\ngot:<\n%s\n>\nwant:<\n%s\n>", strings.Join(out, "\n"), strings.Join(test.wantOutput, "\n"))
				t.Logf("diff: (-clean +maybe with bugs)\n %v", diff)
			}
		})
	}
}
