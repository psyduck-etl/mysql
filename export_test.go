package main

import (
	"testing"
)

func cmpSlice(l, r []any) bool {
	if l == nil {
		return r == nil
	}

	if len(l) != len(r) {
		return false
	}

	for i := range l {
		if l[i] != r[i] {
			return false
		}
	}

	return true
}

func Test_flat(t *testing.T) {
	testcases := [...]struct {
		have [][]any
		want []any
	}{
		{[][]any{{1.0}, {2.0}}, []any{1.0, 2.0}},
		{[][]any{{}, {"hello"}}, []any{"hello"}},
	}

	for _, tc := range testcases {
		f := flat(tc.have)
		if !cmpSlice(tc.want, f) {
			t.Fatalf("flattening %v yields %v, not %v!",
				tc.have, f, tc.want)
		}
	}
}

func Test_queryFormattable(t *testing.T) {
	want := "INSERT IGNORE INTO foo (bar, baz) VALUES (?, ?), (?, ?), (?, ?)"
	conf := &Config{Table: "foo", Fields: []string{"bar", "baz"}, InsertChunkSize: 3}
	if have := queryFormattable(conf.Table, conf.Fields, conf.InsertChunkSize); have != want {
		t.Fatalf("query fmt got '%s', want '%s'", have, want)
	}
}
