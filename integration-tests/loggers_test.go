package main

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jrockway/json-logs/pkg/parse"
	"github.com/logrusorgru/aurora/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLoggers(t *testing.T) {
	startDate := time.Date(2020, 1, 2, 12, 0, 0, 0, time.UTC)
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
				var offset time.Duration
				log := func(msg string, fields ...zap.Field) {
					e := l.Check(zap.InfoLevel, msg)
					e.Time = startDate.Add(offset * 500 * time.Millisecond)
					offset++
					e.Write(fields...)
				}
				log("line 1")
				log("line 2", zap.String("string", "value"), zap.Int("int", 42), zap.Any("object", exampleObject))
				log("line 3", zap.Error(exampleError))
			},
		},
	}

	outs := &parse.OutputSchema{
		Formatter: &parse.DefaultOutputFormatter{
			Aurora:               aurora.NewAurora(false),
			AbsoluteTimeFormat:   "2006-01-02T15:04:05.000Z07:00",
			ElideDuplicateFields: true,
			Zone:                 time.UTC,
		},
	}
	want := `
INFO  2020-01-02T12:00:00.000Z line 1
INFO  2020-01-02T12:00:00.500Z line 2 string:value int:42 object:{"foo":"bar"}
INFO  2020-01-02T12:00:01.000Z line 3 error:whoa!
`
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
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
