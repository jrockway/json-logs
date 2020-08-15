package main

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	joonix "github.com/joonix/log"
	"github.com/jrockway/json-logs/pkg/parse"
	aurora "github.com/logrusorgru/aurora/v3"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ignoreTimeFormatter struct {
	*parse.DefaultOutputFormatter
	i int
}

func (f *ignoreTimeFormatter) FormatTime(s *parse.State, t time.Time, w *bytes.Buffer) error {
	f.i++
	w.WriteString(strconv.Itoa(f.i))
	return nil
}

func TestLoggers(t *testing.T) {
	exampleObject := map[string]interface{}{"foo": "bar"}
	exampleError := errors.New("whoa!")
	testData := []struct {
		name string
		ins  *parse.InputSchema
		f    func(buf *bytes.Buffer)
	}{
		{
			name: "zap",
			ins: &parse.InputSchema{
				LevelKey:    "level",
				MessageKey:  "msg",
				TimeKey:     "ts",
				LevelFormat: parse.DefaultLevelParser,
				TimeFormat:  parse.DefaultTimeParser,
				Strict:      true,
			},
			f: func(buf *bytes.Buffer) {
				enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
				core := zapcore.NewCore(enc, zapcore.Lock(zapcore.AddSync(buf)), zap.DebugLevel)
				l := zap.New(core)
				l.Info("line 1")
				l.Info("line 2", zap.String("string", "value"), zap.Int("int", 42), zap.Any("object", exampleObject))
				l.Info("line 3", zap.Error(exampleError))
			},
		},
		{
			name: "logrus/joonix",
			ins: &parse.InputSchema{
				LevelKey:    "severity",
				MessageKey:  "message",
				TimeKey:     "timestamp",
				LevelFormat: parse.DefaultLevelParser,
				TimeFormat:  parse.DefaultTimeParser,
				Strict:      true,
			},
			f: func(buf *bytes.Buffer) {
				logrus.SetOutput(buf)
				logrus.SetFormatter(joonix.NewFormatter())
				logrus.Info("line 1")
				logrus.WithField("string", "value").WithField("int", 42).WithField("object", exampleObject).Info("line 2")
				logrus.WithError(exampleError).Info("line 3")
			},
		},
	}
	f := &ignoreTimeFormatter{
		DefaultOutputFormatter: &parse.DefaultOutputFormatter{
			Aurora:               aurora.NewAurora(false),
			AbsoluteTimeFormat:   "",
			ElideDuplicateFields: true,
		},
	}
	outs := &parse.OutputSchema{
		PriorityFields: []string{"error", "string", "int", "object"},
		Formatter:      f,
	}
	want := `
INFO  1 line 1
INFO  2 line 2 string:value int:42 object:{"foo":"bar"}
INFO  3 line 3 error:whoa!
`
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			f.i = 0
			outs.EmitErrorFn = func(msg string) { t.Fatalf("emit error fn: %s", msg) }
			input := new(bytes.Buffer)
			output := new(bytes.Buffer)
			test.f(input)
			if _, err := parse.ReadLog(input, output, test.ins, outs, nil); err != nil {
				t.Fatalf("readlog: %v", err)
			}
			if diff := cmp.Diff(output.String(), want[1:]); diff != "" {
				t.Errorf("output:\n%s", diff)
			}
		})
	}
}
