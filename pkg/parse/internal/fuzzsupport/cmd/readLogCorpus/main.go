// Command readLogCorpus reads files named on the command-line, translating them from the fuzzer's
// corpus file format to raw JSON logs.
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"

	"github.com/jrockway/json-logs/pkg/parse/internal/fuzzsupport"
)

const (
	header = "go test fuzz v1\n"
	footer = "\n"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: readLogsCorpus <filename> [<filename> ...]\n")
		os.Exit(2)
	}
	for _, name := range os.Args[1:] {
		logs, err := readFile(name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse %v: %v\n", name, err)
			os.Exit(1)
		}
		os.Stdout.Write(logs)
	}
}

func readFile(name string) ([]byte, error) {
	content, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if !bytes.HasPrefix(content, []byte(header)) {
		return nil, fmt.Errorf("unexpected file format; want header %s, got %s", header, content)
	}
	if !bytes.HasSuffix(content, []byte(footer)) {
		return nil, fmt.Errorf("unexpected file format; want footer %s, got %s", footer, content)
	}
	body := content[len(header) : len(content)-(len(footer))]
	value, err := parseCorpusValue(body)
	if err != nil {
		return nil, fmt.Errorf("parse expr: %w", err)
	}
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("corpus file contained %#v instead of a string", value)
	}
	var in fuzzsupport.JSONLogs
	if err := in.UnmarshalText([]byte(str)); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return in.Data, nil
}

// copied from go/src/internal/fuzz/encoding.go
func parseCorpusValue(line []byte) (any, error) {
	fs := token.NewFileSet()
	expr, err := parser.ParseExprFrom(fs, "(test)", line, 0)
	if err != nil {
		return nil, err
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return nil, fmt.Errorf("expected call expression")
	}
	if len(call.Args) != 1 {
		return nil, fmt.Errorf("expected call expression with 1 argument; got %d", len(call.Args))
	}
	arg := call.Args[0]

	if arrayType, ok := call.Fun.(*ast.ArrayType); ok {
		if arrayType.Len != nil {
			return nil, fmt.Errorf("expected []byte or primitive type")
		}
		elt, ok := arrayType.Elt.(*ast.Ident)
		if !ok || elt.Name != "byte" {
			return nil, fmt.Errorf("expected []byte")
		}
		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return nil, fmt.Errorf("string literal required for type []byte")
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	}

	idType, ok := call.Fun.(*ast.Ident)
	if !ok {
		return nil, fmt.Errorf("expected []byte or primitive type")
	}
	if idType.Name == "bool" {
		id, ok := arg.(*ast.Ident)
		if !ok {
			return nil, fmt.Errorf("malformed bool")
		}
		if id.Name == "true" {
			return true, nil
		} else if id.Name == "false" {
			return false, nil
		} else {
			return nil, fmt.Errorf("true or false required for type bool")
		}
	}
	var (
		val  string
		kind token.Token
	)
	if op, ok := arg.(*ast.UnaryExpr); ok {
		// Special case for negative numbers.
		lit, ok := op.X.(*ast.BasicLit)
		if !ok || (lit.Kind != token.INT && lit.Kind != token.FLOAT) {
			return nil, fmt.Errorf("expected operation on int or float type")
		}
		if op.Op != token.SUB {
			return nil, fmt.Errorf("unsupported operation on int: %v", op.Op)
		}
		val = op.Op.String() + lit.Value // e.g. "-" + "124"
		kind = lit.Kind
	} else {
		lit, ok := arg.(*ast.BasicLit)
		if !ok {
			return nil, fmt.Errorf("literal value required for primitive type")
		}
		val, kind = lit.Value, lit.Kind
	}

	switch typ := idType.Name; typ {
	case "string":
		if kind != token.STRING {
			return nil, fmt.Errorf("string literal value required for type string")
		}
		return strconv.Unquote(val)
	case "byte", "rune":
		if kind != token.CHAR {
			return nil, fmt.Errorf("character literal required for byte/rune types")
		}
		n := len(val)
		if n < 2 {
			return nil, fmt.Errorf("malformed character literal, missing single quotes")
		}
		code, _, _, err := strconv.UnquoteChar(val[1:n-1], '\'')
		if err != nil {
			return nil, err
		}
		if typ == "rune" {
			return code, nil
		}
		if code >= 256 {
			return nil, fmt.Errorf("can only encode single byte to a byte type")
		}
		return byte(code), nil
	// case "int", "int8", "int16", "int32", "int64":
	// 	if kind != token.INT {
	// 		return nil, fmt.Errorf("integer literal required for int types")
	// 	}
	// 	return parseInt(val, typ)
	// case "uint", "uint8", "uint16", "uint32", "uint64":
	// 	if kind != token.INT {
	// 		return nil, fmt.Errorf("integer literal required for uint types")
	// 	}
	// 	return parseUint(val, typ)
	case "float32":
		if kind != token.FLOAT && kind != token.INT {
			return nil, fmt.Errorf("float or integer literal required for float32 type")
		}
		v, err := strconv.ParseFloat(val, 32)
		return float32(v), err
	case "float64":
		if kind != token.FLOAT && kind != token.INT {
			return nil, fmt.Errorf("float or integer literal required for float64 type")
		}
		return strconv.ParseFloat(val, 64)
	default:
		return nil, fmt.Errorf("expected []byte or primitive type")
	}
}
