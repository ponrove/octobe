package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
	"github.com/stretchr/testify/require"
)

func TestPoolMock(t *testing.T) {
	ctx := context.Background()

	t.Run("Ping success", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)

		mock.ExpectPing()
		err = o.Ping(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Ping error", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)

		expectedErr := errors.New("ping failed")
		mock.ExpectPing().WillReturnError(expectedErr)

		err = o.Ping(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Close success", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)

		mock.ExpectClose()
		err = o.Close(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Exec success without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "INSERT INTO events"
		args := []any{1, "test"}
		mock.ExpectExec(query).WithArgs(args...).WillReturnResult(pgconn.CommandTag{})

		_, err = session.Builder()(query).Arguments(args...).Exec()
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Exec error without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

	t.Run("Query success without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT id, name FROM users"
		rows := NewMockRows([]string{"id", "name"}).
			AddRow(1, "John Doe").
			AddRow(2, "Jane Doe")

		mock.ExpectQuery(query).WillReturnRows(rows)

		err = session.Builder()(query).Query(func(r postgres.Rows) error {
			i := 0
			for r.Next() {
				var id int
				var name string
				require.NoError(t, r.Scan(&id, &name))
				require.Equal(t, rows.GetRowsForTesting()[i][0], id)
				require.Equal(t, rows.GetRowsForTesting()[i][1], name)
				i++
			}
			return r.Err()
		})
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Query error without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT id, name FROM users"
		expectedErr := errors.New("query error")
		mock.ExpectQuery(query).WillReturnError(expectedErr)

		err = session.Builder()(query).Query(func(r postgres.Rows) error {
			return nil
		})
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("QueryRow success without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT name FROM users WHERE id = ?"
		row := NewMockRow("John Doe")
		mock.ExpectQueryRow(query).WithArgs(1).WillReturnRow(row)

		var name string
		err = session.Builder()(query).Arguments(1).QueryRow(&name)
		require.NoError(t, err)
		require.Equal(t, "John Doe", name)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("QueryRow error without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT name FROM users WHERE id = ?"
		expectedErr := errors.New("row scan error")
		row := NewMockRow().WillReturnError(expectedErr)
		mock.ExpectQueryRow(query).WithArgs(1).WillReturnRow(row)

		var name string
		err = session.Builder()(query).Arguments(1).QueryRow(&name)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Transaction success", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)

		txOpts := postgres.PGXTxOptions{}
		mock.ExpectBeginTx()
		mock.ExpectCommit()

		session, err := o.Begin(ctx, postgres.WithPGXTxOptions(txOpts))
		require.NoError(t, err)

		err = session.Commit()
		require.NoError(t, err)

		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Transaction with exec", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)

		txOpts := postgres.PGXTxOptions{}
		mock.ExpectBeginTx()
		query := "INSERT INTO users (name) VALUES ($1)"
		mock.ExpectExec(query).WithArgs("test-user").WillReturnResult(pgconn.CommandTag{})
		mock.ExpectCommit()

		session, err := o.Begin(ctx, postgres.WithPGXTxOptions(txOpts))
		require.NoError(t, err)

		_, err = session.Builder()(query).Arguments("test-user").Exec()
		require.NoError(t, err)

		err = session.Commit()
		require.NoError(t, err)

		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Transaction rollback", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)

		txOpts := postgres.PGXTxOptions{}
		mock.ExpectBeginTx()
		mock.ExpectRollback()

		session, err := o.Begin(ctx, postgres.WithPGXTxOptions(txOpts))
		require.NoError(t, err)

		err = session.Rollback()
		require.NoError(t, err)

		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Unfulfilled expectations", func(t *testing.T) {
		mock := NewPGXPoolMock()
		mock.ExpectPing()
		mock.ExpectClose()

		err := mock.AllExpectationsMet()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unfulfilled expectation: method Ping")
	})

	t.Run("No more expectations", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
		require.NoError(t, err)

		err = o.Ping(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNoExpectation)
	})
}
