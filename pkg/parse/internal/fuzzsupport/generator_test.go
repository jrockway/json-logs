package fuzzsupport

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// transformJSONLogStream is a cmp.Transformer that treats a JSONLogStream as a []map[string]any
// instead of bytes.  The input must be a valid stream of JSON logs; a panic occurs if not.
func transformJSONLogStream(in JSONLogStream) []map[string]any {
	var result []map[string]any
	s := bufio.NewScanner(bytes.NewReader(in))
	s.Buffer(make([]byte, 0, 2*1024*1024), 2*1024*1024)
	for s.Scan() {
		l := make(map[string]any)
		if err := json.Unmarshal(s.Bytes(), &l); err != nil {
			panic(fmt.Sprintf("unmarshal in transformer: %v", err))
		}
		result = append(result, l)
	}
	if err := s.Err(); err != nil {
		panic(fmt.Sprintf("line scanner in transformer: %v", err))
	}
	return result
}

// TestUnmarshalText tests that chunks of binary turn into the desired log streams.
func TestUnmarshalText(t *testing.T) {
	testData := []struct {
		name  string
		input string
		want  JSONLogs
	}{
		{
			name:  "empty",
			input: "",
			want: JSONLogs{
				Data:   []byte("{}\n"),
				NLines: 1,
			},
		},
		{
			name:  "many empty lines",
			input: "\x00\x00",
			want: JSONLogs{
				Data:   []byte("{}\n{}\n{}\n"),
				NLines: 3,
			},
		},
		{
			name:  "reasonable one-line log",
			input: "\x01\x04\x07",
			want: JSONLogs{
				Data:   []byte(`{"ts":1234,"level":"info","msg":"hello"}` + "\n"),
				NLines: 1,
			},
		},
		{
			name:  "reasonable multi-line log",
			input: "\x01\x04\x07\xfffoo\x00bar\x00\x00\x01\x04\x07",
			want: JSONLogs{
				Data: []byte(`{"ts":1234,"level":"info","msg":"hello","foo":"bar"}` +
					"\n" +
					`{"ts":1234,"level":"info","msg":"hello"}` +
					"\n"),
				NLines: 2,
			},
		},
		{
			name:  "log with json sub-objects",
			input: "\x03\x06\x08\xffobj\x00{\"foo\":\"bar\"}\x00\xfflist\x00[1,2,\"hello\"]\x00",
			want: JSONLogs{
				Data:   []byte(`{"timestamp":{"seconds":1234,"nanos":4321},"severity":"info","message":"hello","obj":{"foo":"bar"},"list":[1,2,"hello"]}` + "\n"),
				NLines: 1,
			},
		},
		{
			name:  "log with null value",
			input: "\x01\x04\x20",
			want: JSONLogs{
				Data:   []byte(`{"ts":1234,"level":"info","msg":null}` + "\n"),
				NLines: 1,
			},
		},
		{
			name:  "key with NaN value",
			input: "\xffkey\x00NaN\x00",
			want: JSONLogs{
				Data:   []byte(`{"key":"NaN"}`),
				NLines: 1,
			},
		},
		{
			name:  "key with Inf value",
			input: "\xffkey\x00Inf\x00",
			want: JSONLogs{
				Data:   []byte(`{"key":"Inf"}`),
				NLines: 1,
			},
		},
		{
			name:  "key with -Inf value",
			input: "\xffkey\x00-Inf\x00",
			want: JSONLogs{
				Data:   []byte(`{"key":"-Inf"}`),
				NLines: 1,
			},
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var l JSONLogs
			if err := l.UnmarshalText([]byte(test.input)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(l, test.want, cmp.Transformer("logStream", transformJSONLogStream)); diff != "" {
				t.Errorf("generated logs (-got +want)\n%s", diff)
			}
		})
	}
}
