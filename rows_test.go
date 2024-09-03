package athena

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
)

var dummyError = errors.New("dummy error")

type genQueryResultsOutputByToken func(token string) (*athena.GetQueryResultsOutput, error)

var queryToResultsGenMap = map[string]genQueryResultsOutputByToken{
	"select":         dummySelectQueryResponse,
	"show":           dummyShowResponse,
	"iteration_fail": dummyFailedIterationResponse,
}

func genColumnInfo(column string) types.ColumnInfo {
	caseSensitive := true
	catalogName := "hive"
	nullable := types.ColumnNullableUnknown
	precision := int32(2147483647)
	scale := int32(0)
	schemaName := ""
	tableName := ""
	columnType := "varchar"

	return types.ColumnInfo{
		CaseSensitive: caseSensitive,
		CatalogName:   &catalogName,
		Nullable:      nullable,
		Precision:     precision,
		Scale:         scale,
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

func genRow(isHeader bool, columns []types.ColumnInfo) types.Row {
	var data []types.Datum
	for i := 0; i < len(columns); i++ {
		if isHeader {
			data = append(data, types.Datum{
				VarCharValue: columns[i].Name,
			})
		} else {
			s := randomString()
			data = append(data, types.Datum{
				VarCharValue: &s,
			})
		}
	}
	return types.Row{
		Data: data,
	}
}

func dummySelectQueryResponse(token string) (*athena.GetQueryResultsOutput, error) {
	switch token {
	case "":
		var nextToken = "page_1"
		columns := []types.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			NextToken: &nextToken,
			ResultSet: &types.ResultSet{
				ResultSetMetadata: &types.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []types.Row{
					genRow(true, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
				},
			},
		}, nil
	case "page_1":
		columns := []types.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			ResultSet: &types.ResultSet{
				ResultSetMetadata: &types.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []types.Row{
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
	columns := []types.ColumnInfo{
		genColumnInfo("partition"),
	}
	return &athena.GetQueryResultsOutput{
		ResultSet: &types.ResultSet{
			ResultSetMetadata: &types.ResultSetMetadata{
				ColumnInfo: columns,
			},
			Rows: []types.Row{
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
		columns := []types.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			NextToken: &nextToken,
			ResultSet: &types.ResultSet{
				ResultSetMetadata: &types.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []types.Row{
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
	athenaAPI
}

func (m *mockAthenaClient) GetQueryResults(ctx context.Context, query *athena.GetQueryResultsInput, opts ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
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
	ctx := context.Background()
	for _, test := range tests {
		r, _ := newRows(ctx, rowsConfig{
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
