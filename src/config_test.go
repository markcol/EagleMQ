package eaglemq

import (
	"testing"
)

var onofftests = []struct {
	in  string
	ret bool
}{
	{"true", true},
	{"True", true},
	{"TRUE", true},
	{"tRuE", true},
	{"TrUe", true},
	{"On", true},
	{"ON", true},
	{"on", true},
	{"oN", true},
	{"False", false},
	{"false", false},
	{"fAlSe", false},
	{"Off", false},
	{"foobar", false},
}

func TestIsOn(t *testing.T) {
	for i, tt := range onofftests {
		r := isOn(tt.in)
		if r != tt.ret {
			t.Errorf("%d: isOn(%s) returned %q: expected %q", i, tt.in, r, tt.ret)
		}
	}
}
