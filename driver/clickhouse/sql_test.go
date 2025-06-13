package clickhouse_test

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/clickhouse"
	"github.com/stretchr/testify/assert"
)

func TestSQLWithTxInsideStartTransaction(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users WHERE id = ?"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)
	mock.ExpectCommit()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[clickhouse.Builder]) error {
		return session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	}, clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
	assert.NoError(t, err)

	assert.Equal(t, id, destID)
	assert.Equal(t, name, destName)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLWithTx(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users WHERE id = ?"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)
	mock.ExpectCommit()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background(), clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.NoError(t, err)

	assert.Equal(t, id, destID)
	assert.Equal(t, name, destName)

	assert.NoError(t, session.Commit())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLWithoutTx(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users WHERE id = ?"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.NoError(t, err)

	assert.Equal(t, id, destID)
	assert.Equal(t, name, destName)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "SELECT id, name FROM users WHERE id = ?"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	err = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[clickhouse.Builder]) error {
		var (
			destID   int
			destName string
		)
		return session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	}, clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
	assert.ErrorIs(t, err, sql.ErrNoRows)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	defer func() {
		assert.NotNil(t, recover(), "The code did not panic")
		assert.NoError(t, mock.ExpectationsWereMet())
	}()

	_ = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[clickhouse.Builder]) error {
		panic("something went wrong")
	}, clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
}

func TestSQLWithTxManualRollback(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "SELECT id, name FROM users WHERE id = ?"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(context.Background(), clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.Error(t, err)

	assert.NoError(t, session.Rollback())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLWithoutTxCommit(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	err = session.Commit()
	assert.Error(t, err)
	assert.Equal(t, "cannot commit without transaction", err.Error())
}

func TestSQLWithoutTxRollback(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	err = session.Rollback()
	assert.Error(t, err)
	assert.Equal(t, "cannot rollback without transaction", err.Error())
}

func TestSQLSegmentUsedTwice(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users WHERE id = ?"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	q := session.Builder()(query).Arguments(1)
	err = q.QueryRow(&destID, &destName)
	assert.NoError(t, err)

	assert.Equal(t, id, destID)
	assert.Equal(t, name, destName)

	// This should return an error
	err = q.QueryRow(&destID, &destName)
	assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenSQLWithConnNil(t *testing.T) {
	t.Parallel()

	open := clickhouse.OpenWithConn(nil)
	_, err := octobe.New(open)
	assert.Error(t, err)
	assert.Equal(t, "db is nil", err.Error())
}

func TestSQLBeginError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	expectedErr := errors.New("cannot begin")

	mock.ExpectBegin().WillReturnError(expectedErr)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	_, err = instance.Begin(context.Background(), clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLCommitError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	expectedErr := errors.New("cannot commit")

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(expectedErr)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(context.Background(), clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
	assert.NoError(t, err)

	err = session.Commit()
	assert.ErrorIs(t, err, expectedErr)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLSegmentExecError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	query := "INSERT INTO users (id, name) VALUES (?, ?)"
	expectedErr := errors.New("cannot exec")
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(1, "test").WillReturnError(expectedErr)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	_, err = session.Builder()(query).Arguments(1, "test").Exec()
	assert.ErrorIs(t, err, expectedErr)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLSegmentQueryRowError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	query := "SELECT id, name FROM users WHERE id = ?"
	expectedErr := errors.New("cannot query row")
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnError(expectedErr)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.ErrorIs(t, err, expectedErr)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLSegmentQueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	query := "SELECT id, name FROM users"
	expectedErr := errors.New("cannot query")
	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnError(expectedErr)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	err = session.Builder()(query).Query(func(r clickhouse.Rows) error {
		return nil
	})

	assert.ErrorIs(t, err, expectedErr)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLSegmentExecErrorRowsAffected(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	query := "INSERT INTO users (id, name) VALUES (?, ?)"
	expectedErr := errors.New("cannot exec")
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(1, "test").WillReturnResult(sqlmock.NewErrorResult(expectedErr))

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	_, err = session.Builder()(query).Arguments(1, "test").Exec()
	assert.Error(t, err)
}

func TestSQLSegmentExecTxErrorRowsAffected(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	query := "INSERT INTO users (id, name) VALUES (?, ?)"
	expectedErr := errors.New("cannot exec")

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(1, "test").WillReturnResult(sqlmock.NewErrorResult(expectedErr))

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background(), clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))
	assert.NoError(t, err)

	_, err = session.Builder()(query).Arguments(1, "test").Exec()
	assert.Error(t, err)
}

func TestSQLWatchRollback(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	expectedErr := errors.New("some error")

	mock.ExpectBegin()
	mock.ExpectRollback()

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	err = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[clickhouse.Builder]) error {
		return expectedErr
	}, clickhouse.WithClickhouseTxOptions(clickhouse.ClickhouseTxOptions{}))

	assert.ErrorIs(t, err, expectedErr)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLSegmentQueryCloseRowsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users"
	)

	expectedErr := errors.New("cannot query")
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name).CloseError(expectedErr)

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

	open := clickhouse.OpenWithConn(db)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	err = session.Builder()(query).Query(func(r clickhouse.Rows) error {
		return nil
	})

	assert.Error(t, err)
}
