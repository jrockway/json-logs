package parse

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestJQ(t *testing.T) {
	referenceLine := func() *line { return &line{msg: "foo", fields: map[string]interface{}{"foo": 42, "bar": "hi"}} }
	tmpdir, err := os.MkdirTemp("", "jlog-test-jq-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	})
	if err := os.WriteFile(filepath.Join(tmpdir, ".jq"), []byte(`def initFunction: {"init": true};`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpdir, "foo.jq"), []byte(`def no: select($MSG|test("foo")|not);`), 0o600); err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		jq           string
		l            *line
		searchPath   []string
		wantLine     *line
		wantFiltered bool
		wantErr      error
	}{
		{
			jq:           ".",
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: false,
			wantErr:      nil,
		},
		{
			jq:           "",
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: false,
			wantErr:      nil,
		},
		{
			jq:           `error("goodbye")`,
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: false,
			wantErr:      Match("goodbye"),
		},
		{
			jq:           "null",
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: false,
			wantErr:      Match("unexpected nil result"),
		},
		{
			jq:           "3.141592",
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: false,
			wantErr:      Match("unexpected result type float64\\(3.1"),
		},
		{
			jq:           "1 > 2",
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: false,
			wantErr:      Match("unexpected boolean output"),
		},
		{
			jq:           "{}",
			l:            referenceLine(),
			wantLine:     &line{msg: "foo"},
			wantFiltered: false,
			wantErr:      nil,
		},
		{
			jq:           "{}, {}",
			l:            referenceLine(),
			wantLine:     &line{msg: "foo"},
			wantFiltered: false,
			wantErr:      Match("unexpectedly produced more than 1 output"),
		},
		{
			jq:           "empty",
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: true,
			wantErr:      nil,
		},
		{
			jq:           ".",
			searchPath:   []string{filepath.Join(tmpdir, ".jq"), tmpdir},
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: false,
			wantErr:      nil,
		},
		{
			jq:         "initFunction",
			searchPath: []string{filepath.Join(tmpdir, ".jq"), tmpdir},
			l:          referenceLine(),
			wantLine: &line{
				msg:    "foo",
				fields: map[string]interface{}{"init": true},
			},
			wantFiltered: false,
			wantErr:      nil,
		},
		{
			jq:           `import "foo" as foo; foo::no`,
			searchPath:   []string{filepath.Join(tmpdir, ".jq"), tmpdir},
			l:            referenceLine(),
			wantLine:     referenceLine(),
			wantFiltered: true,
			wantErr:      nil,
		},
		{
			jq:       `highlight(true)`,
			l:        referenceLine(),
			wantLine: func() *line { l := referenceLine(); l.highlight = true; return l }(),
		},
		{
			jq:       `highlight(false)`,
			l:        referenceLine(),
			wantLine: referenceLine(),
		},
		{
			jq:       `highlight(42)`,
			l:        referenceLine(),
			wantLine: referenceLine(),
			wantErr:  Match("should be a boolean"),
		},
	}
	for _, test := range testData {
		t.Run(test.jq, func(t *testing.T) {
			fs := new(FilterScheme)
			if err := fs.AddJQ(test.jq, &JQOptions{SearchPath: test.searchPath}); err != nil {
				t.Fatal(err)
			}
			gotFiltered, gotErr := fs.runJQ(test.l)
			if diff := cmp.Diff(test.l, test.wantLine, cmp.AllowUnexported(line{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("line: %s", diff)
			}
			if got, want := gotFiltered, test.wantFiltered; got != want {
				t.Errorf("filtered:\n  got: %v\n want: %v", got, want)
			}
			if got, want := gotErr, test.wantErr; !comperror(got, want) {
				t.Errorf("error:\n  got: %v\n want: %v", got, want)
			}
		})
	}
}

func TestAdds(t *testing.T) {
	testData := []struct {
		name                           string
		match, matchagain, nomatch, jq []string
		want                           []error
	}{
		{
			name: "empty",
		},
		{
			name:  "valid match",
			match: []string{"foo"},
		},
		{
			name:    "valid nomatch",
			nomatch: []string{"foo"},
		},
		{
			name: "valid jq",
			jq:   []string{"{}"},
		},
		{
			name:  "valid match and jq",
			match: []string{"foo"},
			jq:    []string{"{}"},
		},
		{
			name:    "valid nomatch and jq",
			nomatch: []string{"foo"},
			jq:      []string{"{}"},
		},
		{
			name: "unparseable jq",
			jq:   []string{"{"},
			want: []error{Match("EOF")},
		},
		{
			name: "uncompilable jq",
			jq:   []string{"$INVALID"},
			want: []error{Match("variable not defined")},
		},
		{
			name: "double jq",
			jq:   []string{".", "."},
			want: []error{Match("already added")},
		},
		{
			name:  "invalid match",
			match: []string{"["},
			want:  []error{Match("missing closing ]")},
		},
		{
			name:    "invalid nomatch",
			nomatch: []string{"["},
			want:    []error{Match("missing closing ]")},
		},
		{
			name:    "invalid regexes",
			match:   []string{"["},
			nomatch: []string{"["},
			want:    []error{Match("missing closing ]"), Match("missing closing ]")},
		},
		{
			name:    "match and nomatch",
			match:   []string{".*"},
			nomatch: []string{".*"},
			want:    []error{ErrConflict},
		},
		{
			name:       "nomatch and match",
			nomatch:    []string{".*"},
			matchagain: []string{".*"},
			want:       []error{ErrConflict},
		},
		{
			name:  "double match",
			match: []string{"a", "b"},
			want:  []error{ErrAlreadyAdded},
		},
		{
			name:    "double nomatch",
			nomatch: []string{"a", "b"},
			want:    []error{ErrAlreadyAdded},
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			f := new(FilterScheme)
			var errs []error
			for _, jq := range test.jq {
				if err := f.AddJQ(jq, nil); err != nil {
					errs = append(errs, err)
				}
			}
			for _, rx := range test.match {
				if err := f.AddMatchRegex(rx); err != nil {
					errs = append(errs, err)
				}
			}
			for _, rx := range test.nomatch {
				if err := f.AddNoMatchRegex(rx); err != nil {
					errs = append(errs, err)
				}
			}
			for _, rx := range test.matchagain {
				if err := f.AddMatchRegex(rx); err != nil {
					errs = append(errs, err)
				}
			}
			if diff := cmp.Diff(errs, test.want, cmp.Comparer(comperror)); diff != "" {
				t.Errorf("error:\n%s", diff)
			}
			if len(errs) > 0 {
				if _, err := f.Run(&line{}); err != nil {
					t.Errorf("run: %v", err)
				}
			}
		})
	}
}

func TestRegexp(t *testing.T) {
	defaultFields := func(add map[string]string) map[string]any {
		result := map[string]any{
			"string": "string",
			"map": map[string]any{
				"string": "bar",
				"bytes":  []byte("baz"),
				"map": map[string]any{
					"text":   "quux",
					"number": 2,
				},
				"slice": []any{"text", []byte("binary"), 21, 4.321},
			},
			"slice": []any{"string", []byte("bytes"), 42, 1.234},
		}
		for k, v := range add {
			result[k] = v
		}
		return result
	}
	testData := []struct {
		name       string
		pattern    string
		scope      RegexpScope
		wantMatch  bool
		wantFields map[string]any
	}{
		{
			name:      "message scope",
			pattern:   `here is a (?P<what>f(o+)[^[:space:]]+)$`,
			scope:     RegexpScopeMessage,
			wantMatch: true,
			wantFields: defaultFields(map[string]string{
				"what": "foobar",
				"$2":   "oo",
			}),
		},
		{
			name:    "message scope, no match",
			pattern: `this does not match anything`,
			scope:   RegexpScopeMessage,
		},
		{
			name:      "key scope",
			pattern:   `^(st.+)$`,
			scope:     RegexpScopeKeys,
			wantMatch: true,
			wantFields: defaultFields(map[string]string{
				"$1": "string",
			}),
		},
		{
			name:    "key scope, no match",
			pattern: `no key with this name`,
			scope:   RegexpScopeKeys,
		},
		{
			name:      "value scope",
			pattern:   `":\["([^"]+)"`,
			scope:     RegexpScopeValues,
			wantMatch: true,
			wantFields: defaultFields(map[string]string{
				"$1": "text",
			}),
		},
		{
			name:    "value scope, no match",
			pattern: `no values that match this`,
			scope:   RegexpScopeValues,
		},
		{
			name:      "all scopes, targeting message",
			pattern:   `^(here is a foobar)$`,
			scope:     RegexpScopeMessage | RegexpScopeKeys | RegexpScopeValues,
			wantMatch: true,
			wantFields: defaultFields(map[string]string{
				"$1": "here is a foobar",
			}),
		},
		{
			name:      "all scopes, targeting keys",
			pattern:   `^(map)$`,
			scope:     RegexpScopeMessage | RegexpScopeKeys | RegexpScopeValues,
			wantMatch: true,
			wantFields: defaultFields(map[string]string{
				"$1": "map",
			}),
		},
		{
			name:      "all scopes, targeting values",
			pattern:   `(\["text",[^\]]+\])`,
			scope:     RegexpScopeMessage | RegexpScopeKeys | RegexpScopeValues,
			wantMatch: true,
			wantFields: defaultFields(map[string]string{
				"$1": `["text","YmluYXJ5",21,4.321]`,
			}),
		},
		{
			name:    "all scopes, no match",
			pattern: `this does not appear anywhere`,
			scope:   RegexpScopeMessage | RegexpScopeKeys | RegexpScopeValues,
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			rx := regexp.MustCompile(test.pattern)
			var l line
			l.time = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			l.lvl = LevelInfo
			l.msg = "here is a foobar"
			l.raw = []byte(`{"msg":"here is a foobar"}`)
			l.fields = defaultFields(nil)
			if test.wantFields == nil {
				test.wantFields = defaultFields(nil)
			}

			matched := runRegexp(rx, &l, test.scope)
			if got, want := matched, test.wantMatch; got != want {
				t.Errorf("matches?\n  got: %v\n want: %v", got, want)
			}
			if diff := cmp.Diff(l.fields, test.wantFields, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("fields:\n%s", diff)
			}
		})
	}
}

func TestInvalidValues(t *testing.T) {
	rx := regexp.MustCompile(`.`)
	var l line
	l.time = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	l.lvl = LevelInfo
	l.msg = "here is a foobar"
	l.raw = []byte(`{"msg":"here is a foobar"}`)
	l.fields = map[string]any{
		"foo": func() {},
	}
	if got, want := runRegexp(rx, &l, RegexpScopeValues), false; got != want {
		t.Errorf("matches with only 'foo'?\n  got: %v\n want: %v", got, want)
	}
	delete(l.fields, "foo")
	wantFields := map[string]any{
		"jlog_match_marshal_error": "json: unsupported type: func()",
	}
	if diff := cmp.Diff(l.fields, wantFields); diff != "" {
		t.Errorf("fields:\n%s", diff)
	}

	l.fields = map[string]any{
		"foo":        func() {},
		"real thing": "this matches",
	}
	if got, want := runRegexp(rx, &l, RegexpScopeValues), true; got != want {
		t.Errorf("matches with 'real thing'?\n  got: %v\n want: %v", got, want)
	}
}

func TestScopeParsing(t *testing.T) {
	for want := 0; want < RegexpScopeKeys|RegexpScopeValues|RegexpScopeMessage; want++ {
		var got RegexpScope
		s := RegexpScope(want).MarshalFlag()
		if err := got.UnmarshalFlag(s); err != nil {
			t.Fatalf("unmarshal scope %q (%v): %v", s, want, err)
		}
		if got != RegexpScope(want) {
			t.Errorf("scope %q (%v): unmarshals to %v", s, want, got)
		}
	}

	var x RegexpScope
	if err := x.UnmarshalFlag("this is invalid"); err == nil {
		t.Errorf("expected error")
	}
}
