// Package fuzzsupport supports generating random syntactically-sound JSON logs.
package fuzzsupport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// generatorState represents the state of the log generator state machine.
type generatorState int

const (
	stateDefault    generatorState = iota // The next byte is a generator instruction.
	stateKeyBytes                         // The next byte is a key byte, until \0.
	stateValueBytes                       // The next byte is a value byte, until \0.
)

// cannedValues are interpretations of bytes in the stateDefault state.  For example, if the byte
// read in stateDefault is 1, then we add {"ts":1234} to the current log line, and then treat the
// next byte as an additional instruction.
var cannedValues = []struct {
	key, value string
	nextState  generatorState
}{
	{}, // 0: handled by code
	{"ts", "1234", stateDefault},
	{"time", "2022-01-01T00:00:00.123", stateDefault},
	{"timestamp", `{"seconds":1234,"nanos":4321}`, stateDefault},
	{"level", "info", stateDefault},
	{"level", "10", stateDefault},
	{"severity", "info", stateDefault},
	{"msg", "hello", stateDefault},
	{"message", "hello", stateDefault},
	{"error", "failed to foo", stateDefault},
	{"v", "0", stateDefault},
	{"ts", "", stateValueBytes},
	{"time", "", stateValueBytes},
	{"timestamp", "", stateValueBytes},
	{"level", "", stateValueBytes},
	{"severity", "", stateValueBytes},
	{"msg", "", stateValueBytes},
	{"message", "", stateValueBytes},
	{"error", "", stateValueBytes},
	{"v", "", stateValueBytes},
	{"obj", `{"foo":"bar","list":[1,2,"hello"]}`, stateDefault},
	{"level", "invalid", stateDefault},
	{"level", "fatal", stateDefault},
	{"level", "ERROR", stateDefault},
	{"level", "null", stateDefault},
	{"ts", "-1234", stateDefault},
	{"ts", "null", stateDefault},
	{"timestamp", "null", stateDefault},
	{"msg", "{}", stateDefault},
	{"msg", "[1]", stateDefault},
	{"msg", `{"key":"value"}`, stateDefault},
	{"msg", "contains\na newline", stateDefault},
	{"msg", "null", stateDefault},
	{"error", "contains\na newline", stateDefault},
	{"error", "null", stateDefault},
	{"ts", "not a time", stateDefault},
	{"ts", `{"x":42}`, stateDefault},
	{"ts", "contains\na newline", stateDefault},
	{"v", "-1", stateDefault},
	{"v", "null", stateDefault},
}

// JSONLogStream is an alias for []byte so that a cmp.Transformer can be used in tests.
type JSONLogStream []byte

// JSONLogs is a sequence of JSON logs.
type JSONLogs struct {
	Data   JSONLogStream
	NLines int
}

// UnmarshalText turns a particular binary format (described by the code below ;) into a stream of
// JSON logs.
func (l *JSONLogs) UnmarshalText(in []byte) error {
	buf := new(bytes.Buffer)
	var nLines int
	var state generatorState
	var keyBytes, valueBytes []byte
	line := map[string]any{}
	for _, b := range in {
		switch state {
		case stateDefault:
			switch {
			case b == 0:
				// Start new line.
				if err := appendJSON(buf, line); err != nil {
					return fmt.Errorf("append intermediate json line: %w", err)
				}
				nLines++
				line = map[string]any{}
			case int(b) < len(cannedValues):
				// Use a canned expression.
				spec := cannedValues[b]
				keyBytes = []byte(spec.key)
				valueBytes = []byte(spec.value)
				if spec.nextState == stateDefault {
					appendKV(line, keyBytes, valueBytes)
				}
				state = spec.nextState
			default:
				// Start collecting key bytes.
				keyBytes, valueBytes = nil, nil
				state = stateKeyBytes
			}
		case stateKeyBytes:
			if b == 0 {
				state = stateValueBytes
			} else {
				keyBytes = append(keyBytes, b)
			}
		case stateValueBytes:
			if b == 0 {
				state = stateDefault
				appendKV(line, keyBytes, valueBytes)
				keyBytes, valueBytes = nil, nil
			} else {
				valueBytes = append(valueBytes, b)
			}
		}
	}
	if len(keyBytes) > 0 {
		appendKV(line, keyBytes, valueBytes)
	}
	if err := appendJSON(buf, line); err != nil {
		return fmt.Errorf("append final json line: %w", err)
	}
	nLines++
	l.NLines = nLines
	l.Data = buf.Bytes()
	return nil
}

// appendJSON appends a JSON log line to the provided buffer.
func appendJSON(buf *bytes.Buffer, js map[string]any) error {
	x, err := json.Marshal(js)
	if err != nil {
		return fmt.Errorf("marshal line: %w", err)
	}
	buf.Write(x)
	buf.WriteString("\n")
	return nil
}

// appendKV adds to provided key and value to the map.
func appendKV(js map[string]any, key, value []byte) {
	f, err := strconv.ParseFloat(string(value), 64)
	if err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) { // NaN and Inf can't be marshaled to JSON, causing problems later.
		js[string(key)] = f
		return
	}
	if bytes.Equal(value, []byte("null")) {
		js[string(key)] = nil
		return
	}
	if len(value) > 0 && (value[0] == '{' || value[0] == '[') {
		var val any
		if err := json.Unmarshal(value, &val); err == nil {
			js[string(key)] = val
			return
		}
	}
	js[string(key)] = string(value)
}
