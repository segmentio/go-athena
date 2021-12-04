package athena

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConvertValue(t *testing.T) {
	testData := map[string]struct {
		AthenaType     string
		RawValue       string
		ExpectedOutput interface{}
		ExpectError    bool
	}{
		"smallint":                 {"smallint", "-42", int64(-42), false},
		"smallint too big":         {"smallint", "4294967338", 0, true},
		"integer":                  {"integer", "23000", int64(23000), false},
		"bigint":                   {"bigint", "9223372036854775807", int64(9223372036854775807), false},
		"boolean true":             {"boolean", "true", true, false},
		"boolean false":            {"boolean", "false", false, false},
		"boolean invalid":          {"boolean", "foobar", false, true},
		"float":                    {"float", "23.5", 23.5, false},
		"double":                   {"double", "42.23", 42.23, false},
		"string":                   {"string", "hello world", "hello world", false},
		"timestamp":                {"timestamp", "2021-01-07 16:30:05.321", time.Date(2021, time.January, 7, 16, 30, 05, 321000000, time.UTC), false},
		"timestamp with time zone": {"timestamp with time zone", "2021-01-07 16:40:01.123 UTC", time.Date(2021, time.January, 7, 16, 40, 1, 123000000, time.UTC), false},
		"date":                     {"date", "2015-05-09", time.Date(2015, time.May, 9, 0, 0, 0, 0, time.UTC), false},
		"varbinary":                {"varbinary", "2c 4a 4b 4c 2f", []byte{0x2c, 0x4a, 0x4b, 0x4c, 0x2f}, false},
		"varbinary invalid":        {"varbinary", "invalid", nil, true},
		"row":                      {"row", "{foo=x,bar=y}", nil, true},
		"invalid type":             {"invalid type", "", nil, true},
	}

	for testName, tt := range testData {
		t.Run(testName, func(t *testing.T) {
			output, err := convertValue(tt.AthenaType, &tt.RawValue)
			if tt.ExpectError {
				require.Error(t, err)
				t.Logf("err = %v", err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.ExpectedOutput, output)
			}
		})
	}
}
