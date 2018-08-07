package athena

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/athena"
)

const (
	// TimestampLayout is the Go time layout string for an Athena `timestamp`.
	TimestampLayout = "2006-01-02 15:04:05.999"
)

func convertRow(columns []*athena.ColumnInfo, in []*athena.Datum, ret []driver.Value) error {
	for i, val := range in {
		coerced, err := convertValue(*columns[i].Type, val.VarCharValue)
		if err != nil {
			return err
		}

		ret[i] = coerced
	}

	return nil
}

func convertValue(athenaType string, rawValue *string) (interface{}, error) {
	if rawValue == nil {
		return nil, nil
	}

	val := *rawValue
	switch athenaType {
	case "smallint":
		// TODO: handle errors gracefully
		return strconv.ParseInt(val, 10, 16)
	case "integer":
		// TODO: handle errors gracefully
		return strconv.ParseInt(val, 10, 32)
	case "bigint":
		// TODO: handle errors gracefully
		return strconv.ParseInt(val, 10, 64)
	case "boolean":
		switch val {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("cannot parse '%s' as boolean", val)
	case "float":
		// TODO: handle NaN, Infinity and errors gracefully
		return strconv.ParseFloat(val, 32)
	case "double":
		// TODO: handle NaN, Infinity and errors gracefully
		return strconv.ParseFloat(val, 64)
	case "varchar", "string":
		return val, nil
	case "timestamp":
		// TODO: handle errors gracefully
		return time.Parse(TimestampLayout, val)
	case "array", "map", "row": // gracefully handle these complex types as strings
		return val, nil
	case "json":
		var v interface{}
		err := json.Unmarshal([]byte(val), &v)
		return v, err
	default:
		panic(fmt.Errorf("unknown type `%s` with value %s", athenaType, val))
	}
}
