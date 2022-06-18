package parse

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestContext(t *testing.T) {
	testData := []struct {
		name          string
		before, after int
		match         *regexp.Regexp
		input         []string
		want          []string
	}{
		{
			name:   "select none",
			before: 2,
			after:  2,
			match:  regexp.MustCompile(`^never matches$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{},
		},
		{
			name:   "select all",
			before: 2,
			after:  2,
			match:  regexp.MustCompile(`^.*$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
		},
		{
			name:   "no context, single match",
			before: 0,
			after:  0,
			match:  regexp.MustCompile(`^5$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"5"},
		},
		{
			name:   "no context, two matches",
			before: 0,
			after:  0,
			match:  regexp.MustCompile(`^5|8$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"5", "8"},
		},
		{
			name:   "no context, two contiguous matches",
			before: 0,
			after:  0,
			match:  regexp.MustCompile(`^5|6$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"5", "6"},
		},
		{
			name:   "basic context, single match",
			before: 2,
			after:  2,
			match:  regexp.MustCompile(`^5$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"3", "4", "5", "6", "7"},
		},
		{
			name:   "basic context, single match, at start",
			before: 2,
			after:  2,
			match:  regexp.MustCompile(`^1$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"1", "2", "3"},
		},
		{
			name:   "basic context, single match, at second element",
			before: 2,
			after:  2,
			match:  regexp.MustCompile(`^2$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"1", "2", "3", "4"},
		},
		{
			name:   "basic context, separated match regions",
			before: 1,
			after:  1,
			match:  regexp.MustCompile(`^5|9$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"4", "5", "6", "---", "8", "9", "10"},
		},
		{
			name:   "basic context, contiguous match regions",
			before: 1,
			after:  1,
			match:  regexp.MustCompile(`^5|8$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"4", "5", "6", "7", "8", "9"},
		},
		{
			name:   "after context, separated match regions",
			before: 0,
			after:  2,
			match:  regexp.MustCompile(`^2|8$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"2", "3", "4", "---", "8", "9", "10"},
		},
		{
			name:   "after context, contiguous match regions",
			before: 0,
			after:  2,
			match:  regexp.MustCompile(`^2|3$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"2", "3", "4", "5"},
		},
		{
			name:   "before context, separated match regions",
			before: 2,
			after:  0,
			match:  regexp.MustCompile(`^4|9$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"2", "3", "4", "---", "7", "8", "9"},
		},
		{
			name:   "before context, contiguous match regions",
			before: 2,
			after:  0,
			match:  regexp.MustCompile(`^5|8$`),
			input:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			want:   []string{"3", "4", "5", "6", "7", "8"},
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			ctx := &context{
				Before: test.before,
				After:  test.after,
			}
			out := new(bytes.Buffer)
			var l line // ensure that our stored pointers do enough copying
			for _, msg := range test.input {
				l.reset()
				l.msg = msg
				selected := test.match.MatchString(msg)
				print := ctx.Print(&l, selected)
				for _, x := range print {
					if x.isSeparator {
						out.WriteString("---")
					} else {
						out.WriteString(x.msg)
					}
					out.WriteByte('\n')
				}
			}

			gotOutput := out.String()
			var got []string
			if len(gotOutput) > 0 {
				got = strings.Split(gotOutput, "\n")
			}
			if len(got) > 0 && got[len(got)-1] == "" {
				got = got[:len(got)-1]
			}
			if diff := cmp.Diff(got, test.want, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("output:\n  got: %v\n want: %v", got, test.want)
				t.Logf("diff:\n%s", diff)
			}
		})
	}
}
