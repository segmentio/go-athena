package athena

import (
	"database/sql/driver"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/stretchr/testify/assert"
)

var dummyError = errors.New("dummy error")

type genQueryResultsOutputByToken func(token string) (*athena.GetQueryResultsOutput, error)

var queryToResultsGenMap = map[string]genQueryResultsOutputByToken{
	"select":         dummySelectQueryResponse,
	"show":           dummyShowResponse,
	"iteration_fail": dummyFailedIterationResponse,
}

func genColumnInfo(column string) *athena.ColumnInfo {
	caseSensitive := true
	catalogName := "hive"
	nullable := "UNKNOWN"
	precision := int64(2147483647)
	scale := int64(0)
	schemaName := ""
	tableName := ""
	columnType := "varchar"

	return &athena.ColumnInfo{
		CaseSensitive: &caseSensitive,
		CatalogName:   &catalogName,
		Nullable:      &nullable,
		Precision:     &precision,
		Scale:         &scale,
		SchemaName:    &schemaName,
		TableName:     &tableName,
		Type:          &columnType,
		Label:         &column,
		Name:          &column,
	}
}

func randomString() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	s := make([]byte, 10)
	for i := 0; i < len(s); i++ {
		s[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(s)
}

func genRow(isHeader bool, columns []*athena.ColumnInfo) *athena.Row {
	var data []*athena.Datum
	for i := 0; i < len(columns); i++ {
		if isHeader {
			data = append(data, &athena.Datum{
				VarCharValue: columns[i].Name,
			})
		} else {
			s := randomString()
			data = append(data, &athena.Datum{
				VarCharValue: &s,
			})
		}
	}
	return &athena.Row{
		Data: data,
	}
}

func dummySelectQueryResponse(token string) (*athena.GetQueryResultsOutput, error) {
	switch token {
	case "":
		var nextToken = "page_1"
		columns := []*athena.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			NextToken: &nextToken,
			ResultSet: &athena.ResultSet{
				ResultSetMetadata: &athena.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []*athena.Row{
					genRow(true, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
				},
			},
		}, nil
	case "page_1":
		columns := []*athena.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			ResultSet: &athena.ResultSet{
				ResultSetMetadata: &athena.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []*athena.Row{
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
				},
			},
		}, nil
	default:
		return nil, dummyError
	}
}

func dummyShowResponse(_ string) (*athena.GetQueryResultsOutput, error) {
	columns := []*athena.ColumnInfo{
		genColumnInfo("partition"),
	}
	return &athena.GetQueryResultsOutput{
		ResultSet: &athena.ResultSet{
			ResultSetMetadata: &athena.ResultSetMetadata{
				ColumnInfo: columns,
			},
			Rows: []*athena.Row{
				genRow(false, columns),
				genRow(false, columns),
			},
		},
	}, nil
}

func dummyFailedIterationResponse(token string) (*athena.GetQueryResultsOutput, error) {
	switch token {
	case "":
		var nextToken = "page_1"
		columns := []*athena.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			NextToken: &nextToken,
			ResultSet: &athena.ResultSet{
				ResultSetMetadata: &athena.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []*athena.Row{
					genRow(true, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
				},
			},
		}, nil
	default:
		return nil, dummyError
	}
}

type mockAthenaClient struct {
	athenaiface.AthenaAPI
}

func (m *mockAthenaClient) GetQueryResults(query *athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error) {
	var nextToken = ""
	if query.NextToken != nil {
		nextToken = *query.NextToken
	}
	return queryToResultsGenMap[*query.QueryExecutionId](nextToken)
}

func castToValue(dest ...driver.Value) []driver.Value {
	return dest
}

func TestRows_Next(t *testing.T) {
	tests := []struct {
		desc                string
		queryID             string
		skipHeader          bool
		expectedResultsSize int
		expectedError       error
	}{
		{
			desc:                "show query, no header, 2 rows, no error",
			queryID:             "show",
			skipHeader:          false,
			expectedResultsSize: 2,
			expectedError:       nil,
		},
		{
			desc:                "select query, header, multipage, 9 rows, no error",
			queryID:             "select",
			skipHeader:          true,
			expectedResultsSize: 9,
			expectedError:       nil,
		},
		{
			desc:          "failed during calling next",
			queryID:       "iteration_fail",
			skipHeader:    true,
			expectedError: dummyError,
		},
	}
	for _, test := range tests {
		r, _ := newRows(rowsConfig{
			Athena:     new(mockAthenaClient),
			QueryID:    test.queryID,
			SkipHeader: test.skipHeader,
		})

		var firstName, lastName string
		cnt := 0
		for {
			err := r.Next(castToValue(&firstName, &lastName))
			if err != nil {
				if err != io.EOF {
					assert.Equal(t, test.expectedError, err)
				}
				break
			}
			cnt++
		}
		if test.expectedError == nil {
			assert.Equal(t, test.expectedResultsSize, cnt)
		}
	}
}
