package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
	"github.com/stretchr/testify/require"
)

func TestSQLMock(t *testing.T) {
	ctx := context.Background()

	t.Run("Ping success", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)

		mock.ExpectPing()
		err = o.Ping(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Ping error", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)

		expectedErr := errors.New("ping failed")
		mock.ExpectPing().WillReturnError(expectedErr)

		err = o.Ping(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Close success", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)

		mock.ExpectClose().WillReturnError(nil)
		err = o.Close(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Close error", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)

		expectedErr := errors.New("close error")
		mock.ExpectClose().WillReturnError(expectedErr)

		err = o.Close(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Exec success", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)
		// This will create a non-transactional session
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "INSERT INTO events"
		args := []any{1, "test"}
		result := NewSQLResult(1, 1)
		mock.ExpectExec(query).WithArgs(args...).WillReturnResult(result)

		res, err := session.Builder()(query).Arguments(args...).Exec()
		require.NoError(t, err)
		require.Equal(t, int64(1), res.RowsAffected)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Exec error", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "INSERT INTO events"
		expectedErr := errors.New("exec error")
		mock.ExpectExec(query).WillReturnError(expectedErr)

		_, err = session.Builder()(query).Exec()
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	// NOTE: Testing Query, QueryRow, and transactions with SQLMock is not feasible
	// because `database/sql` returns concrete types (*sql.Rows, *sql.Row, *sql.Tx)
	// which cannot be easily mocked without a full driver mock like go-sqlmock.
	// The current SQLMock implementation will panic for these methods.
	t.Run("Query panics", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT 1"
		mock.ExpectQuery(query)

		require.Panics(t, func() {
			_ = session.Builder()(query).Query(func(r postgres.Rows) error {
				return nil
			})
		})
	})

	t.Run("Unfulfilled expectations", func(t *testing.T) {
		mock := NewSQLMock()
		mock.ExpectPing()
		mock.ExpectClose()

		err := mock.AllExpectationsMet()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unfulfilled expectation: method PingContext")
	})

	t.Run("No more expectations", func(t *testing.T) {
		mock := NewSQLMock()
		o, err := octobe.New(postgres.OpenWithConn(mock))
		require.NoError(t, err)

		err = o.Ping(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNoExpectation)
	})
}
