package athena

import (
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/pointer"
	"github.com/stretchr/testify/assert"
)

func TestConvertValue(t *testing.T) {
	spec := []struct {
		atype    string
		input    *string
		expected interface{}
		err      bool
	}{
		{"smallint", nil, nil, false},
		{"smallint", pointer.String(""), nil, true},
		{"smallint", pointer.String("0"), 0, false},
		{"smallint", pointer.String("-1"), -1, false},
		{"smallint", pointer.String("1"), 1, false},

		{"integer", nil, nil, false},
		{"integer", pointer.String(""), nil, true},
		{"integer", pointer.String("0"), 0, false},
		{"integer", pointer.String("-1"), -1, false},
		{"integer", pointer.String("1"), 1, false},
		{"integer", pointer.String("1000000"), 1000000, false},

		{"bigint", nil, nil, false},
		{"bigint", pointer.String(""), nil, true},
		{"bigint", pointer.String("0"), 0, false},
		{"bigint", pointer.String("-1"), -1, false},
		{"bigint", pointer.String("1"), 1, false},
		{"bigint", pointer.String("1000000000000"), 1000000000000, false},

		{"boolean", nil, nil, false},
		{"boolean", pointer.String(""), nil, true},
		{"boolean", pointer.String("false"), false, false},
		{"boolean", pointer.String("true"), true, false},
		{"boolean", pointer.String("hello world"), nil, true},

		{"float", nil, nil, false},
		{"float", pointer.String(""), nil, true},
		{"float", pointer.String("1.0"), 1.0, false},
		{"float", pointer.String("-2.5"), -2.5, false},
		// FIXME: {"float", pointer.String("NaN"), nil, false},

		{"double", nil, nil, false},
		{"double", pointer.String(""), nil, true},
		{"double", pointer.String("1.0"), 1.0, false},
		{"double", pointer.String("-2.5"), -2.5, false},
		// FIXME: {"double", pointer.String("NaN"), nil, false},

		{"varchar", nil, nil, false},
		{"varchar", pointer.String(""), "", false},
		{"varchar", pointer.String("hello world"), "hello world", false},

		{"string", nil, nil, false},
		{"string", pointer.String(""), "", false},
		{"string", pointer.String("hello world"), "hello world", false},

		{"timestamp", nil, nil, false},
		{"timestamp", pointer.String(""), nil, true},
		{"timestamp", pointer.String("2018-08-07 11:55:00.000"), time.Date(2018, time.August, 7, 11, 55, 0, 0, time.UTC), false},
		{"timestamp", pointer.String("invalid"), nil, true},

		{"array", nil, nil, false},
		{"array", pointer.String(""), "", false},
		{"array", pointer.String("[a, b, c]"), "[a, b, c]", false},

		{"map", nil, nil, false},
		{"map", pointer.String(""), "", false},
		{"map", pointer.String("{hello=world}"), "{hello=world}", false},

		{"json", nil, nil, false},
		{"json", pointer.String(""), nil, true},
		{"json", pointer.String(`{"hello":"world"}`), map[string]interface{}{"hello": "world"}, false},
		{"json", pointer.String("null"), nil, false},
	}

	for _, test := range spec {
		var msg string
		if test.input == nil {
			msg = fmt.Sprintf("converting %v to %s", test.input, test.atype)
		} else {
			msg = fmt.Sprintf("converting '%v' to %s", *test.input, test.atype)
		}

		v, err := convertValue(test.atype, test.input)
		if test.err {
			assert.Errorf(t, err, "expected error %s", msg)
		} else {
			assert.NoErrorf(t, err, "expected no error %s", msg)
			assert.EqualValuesf(t, test.expected, v, msg)
		}
	}
}
