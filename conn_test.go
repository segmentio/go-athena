package athena

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryWithMockAPI(t *testing.T) {
	ctx := context.Background()

	drv := NewDriver(&Config{
		Athena: &mockAthenaClient{},
	})

	sql.Register("athena_test", drv)

	conn, err := sql.Open("athena_test", "")
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "select")
	require.NoError(t, err)

	count := 0

	for rows.Next() {
		var firstName, lastName string
		err := rows.Scan(&firstName, &lastName)
		require.NoError(t, err)
		t.Logf("%d: %s %s", count, firstName, lastName)
		count++
	}

	require.Equal(t, 9, count)
}
