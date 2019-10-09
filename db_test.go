package athena

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	AthenaDatabase = "go_athena_tests"
	S3Bucket       = "go-athena-tests"
)

func init() {
	if v := os.Getenv("ATHENA_DATABASE"); v != "" {
		AthenaDatabase = v
	}

	if v := os.Getenv("S3_BUCKET"); v != "" {
		S3Bucket = v
	}
}

func TestQuery(t *testing.T) {
	harness := setup(t)
	// defer harness.teardown()

	expected := []dummyRow{
		{
			SmallintType:  1,
			IntType:       2,
			BigintType:    3,
			BooleanType:   true,
			FloatType:     3.14159,
			DoubleType:    1.32112345,
			StringType:    "some string",
			TimestampType: athenaTimestamp(time.Date(2006, 1, 2, 3, 4, 11, 0, time.UTC)),
			DateType:      athenaDate(time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)),
			DecimalType:   1001,
		},
		{
			SmallintType:  9,
			IntType:       8,
			BigintType:    0,
			BooleanType:   false,
			FloatType:     3.14159,
			DoubleType:    1.235,
			StringType:    "another string",
			TimestampType: athenaTimestamp(time.Date(2017, 12, 3, 1, 11, 12, 0, time.UTC)),
			DateType:      athenaDate(time.Date(2017, 12, 3, 0, 0, 0, 0, time.UTC)),
			DecimalType:   0,
		},
		{
			SmallintType:  9,
			IntType:       8,
			BigintType:    0,
			BooleanType:   false,
			DoubleType:    1.235,
			FloatType:     3.14159,
			StringType:    "another string",
			TimestampType: athenaTimestamp(time.Date(2017, 12, 3, 20, 11, 12, 0, time.UTC)),
			DateType:      athenaDate(time.Date(2017, 12, 3, 0, 0, 0, 0, time.UTC)),
			DecimalType:   0.48,
		},
	}
	expectedTypeNames := []string{"varchar", "smallint", "integer", "bigint", "boolean", "float", "double", "varchar", "timestamp", "date", "decimal"}
	harness.uploadData(expected)

	rows := harness.mustQuery("select * from %s", harness.table)
	index := -1
	for rows.Next() {
		index++

		var row dummyRow
		require.NoError(t, rows.Scan(
			&row.NullValue,

			&row.SmallintType,
			&row.IntType,
			&row.BigintType,
			&row.BooleanType,
			&row.FloatType,
			&row.DoubleType,
			&row.StringType,
			&row.TimestampType,
			&row.DateType,
			&row.DecimalType,
		))

		assert.Equal(t, expected[index], row, fmt.Sprintf("index: %d", index))

		types, err := rows.ColumnTypes()
		assert.NoError(t, err, fmt.Sprintf("index: %d", index))
		for i, colType := range types {
			typeName := colType.DatabaseTypeName()
			assert.Equal(t, expectedTypeNames[i], typeName, fmt.Sprintf("index: %d", index))
		}
	}

	require.NoError(t, rows.Err(), "rows.Err()")
	require.Equal(t, 3, index+1, "row count")
}

func TestOpen(t *testing.T) {
	db, err := Open(Config{
		Session:        session.Must(session.NewSession()),
		Database:       AthenaDatabase,
		OutputLocation: fmt.Sprintf("s3://%s/noop", S3Bucket),
	})
	require.NoError(t, err, "Open")

	_, err = db.Query("SELECT 1")
	require.NoError(t, err, "Query")
}

type dummyRow struct {
	NullValue     *struct{}       `json:"nullValue"`
	SmallintType  int             `json:"smallintType"`
	IntType       int             `json:"intType"`
	BigintType    int             `json:"bigintType"`
	BooleanType   bool            `json:"booleanType"`
	FloatType     float32         `json:"floatType"`
	DoubleType    float64         `json:"doubleType"`
	StringType    string          `json:"stringType"`
	TimestampType athenaTimestamp `json:"timestampType"`
	DateType      athenaDate      `json:"dateType"`
	DecimalType   float64         `json:"decimalType"`
}

type athenaHarness struct {
	t  *testing.T
	db *sql.DB
	s3 *s3.S3

	table string
}

func setup(t *testing.T) *athenaHarness {
	harness := athenaHarness{t: t, s3: s3.New(session.New())}

	var err error
	harness.db, err = sql.Open("athena", fmt.Sprintf("db=%s&output_location=s3://%s/output", AthenaDatabase, S3Bucket))
	require.NoError(t, err)

	harness.setupTable()

	return &harness
}

func (a *athenaHarness) setupTable() {
	// tables cannot start with numbers or contain dashes
	id := uuid.NewV4()
	a.table = "t_" + strings.Replace(id.String(), "-", "_", -1)
	a.mustExec(`CREATE EXTERNAL TABLE %[1]s (
	nullValue string,
	smallintType smallint,
	intType int,
	bigintType bigint,
	booleanType boolean,
	floatType float,
	doubleType double,
	stringType string,
	timestampType timestamp,
	dateType date,
	decimalType decimal(11, 5)
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
	'serialization.format' = '1'
) LOCATION 's3://%[2]s/%[1]s/';`, a.table, S3Bucket)
	fmt.Printf("created table: %s", a.table)
}

func (a *athenaHarness) teardown() {
	a.mustExec("drop table %s", a.table)
}

func (a *athenaHarness) mustExec(sql string, args ...interface{}) {
	query := fmt.Sprintf(sql, args...)
	_, err := a.db.ExecContext(context.TODO(), query)
	require.NoError(a.t, err, query)
}

func (a *athenaHarness) mustQuery(sql string, args ...interface{}) *sql.Rows {
	query := fmt.Sprintf(sql, args...)
	rows, err := a.db.QueryContext(context.TODO(), query)
	require.NoError(a.t, err, query)
	return rows
}

func (a *athenaHarness) uploadData(rows []dummyRow) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		err := enc.Encode(row)
		require.NoError(a.t, err)
	}

	_, err := a.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String(fmt.Sprintf("%s/fixture.json", a.table)),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	require.NoError(a.t, err)
}

type athenaTimestamp time.Time

func (t athenaTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t athenaTimestamp) String() string {
	return time.Time(t).Format(TimestampLayout)
}

func (t athenaTimestamp) Equal(t2 athenaTimestamp) bool {
	return time.Time(t).Equal(time.Time(t2))
}

type athenaDate time.Time

func (t athenaDate) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t athenaDate) String() string {
	return time.Time(t).Format(DateLayout)
}

func (t athenaDate) Equal(t2 athenaDate) bool {
	return time.Time(t).Equal(time.Time(t2))
}
