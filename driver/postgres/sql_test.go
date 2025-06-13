package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
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
		query = "SELECT id, name FROM users WHERE id = \\$1"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)
	mock.ExpectCommit()

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	var (
		destID   int
		destName string
	)
	err = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[postgres.Builder]) error {
		return session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	}, postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
	if err != nil {
		t.Fatal(err)
	}

	if destID != id {
		t.Errorf("expected id %d, got %d", id, destID)
	}

	if destName != name {
		t.Errorf("expected name %s, got %s", name, destName)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
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
		query = "SELECT id, name FROM users WHERE id = \\$1"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)
	mock.ExpectCommit()

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background(), postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
	if err != nil {
		t.Fatal(err)
	}

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	if err != nil {
		t.Fatal(err)
	}

	if destID != id {
		t.Errorf("expected id %d, got %d", id, destID)
	}

	if destName != name {
		t.Errorf("expected name %s, got %s", name, destName)
	}

	if err := session.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
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
		query = "SELECT id, name FROM users WHERE id = \\$1"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	if err != nil {
		t.Fatal(err)
	}

	if destID != id {
		t.Errorf("expected id %d, got %d", id, destID)
	}

	if destName != name {
		t.Errorf("expected name %s, got %s", name, destName)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "SELECT id, name FROM users WHERE id = \\$1"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	err = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[postgres.Builder]) error {
		var (
			destID   int
			destName string
		)
		return session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	}, postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected error sql.ErrNoRows, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
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

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	_ = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[postgres.Builder]) error {
		panic("something went wrong")
	}, postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
}

func TestSQLWithTxManualRollback(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "SELECT id, name FROM users WHERE id = \\$1"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	session, err := instance.Begin(context.Background(), postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
	if err != nil {
		t.Fatal(err)
	}

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := session.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLWithoutTxCommit(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := session.Commit(); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSQLWithoutTxRollback(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := session.Rollback(); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSQLSegmentUsedTwice(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users WHERE id = \\$1"
	)

	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnRows(rows)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var (
		destID   int
		destName string
	)
	q := session.Builder()(query).Arguments(1)
	err = q.QueryRow(&destID, &destName)
	if err != nil {
		t.Fatal(err)
	}

	if destID != id {
		t.Errorf("expected id %d, got %d", id, destID)
	}

	if destName != name {
		t.Errorf("expected name %s, got %s", name, destName)
	}

	// This should return an error
	err = q.QueryRow(&destID, &destName)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, octobe.ErrAlreadyUsed) {
		t.Fatalf("expected error %v, got %v", octobe.ErrAlreadyUsed, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestOpenSQLWithConnNil(t *testing.T) {
	t.Parallel()

	open := postgres.OpenWithConn(nil)
	_, err := octobe.New(open)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSQLBeginError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	expectedErr := errors.New("cannot begin")

	mock.ExpectBegin().WillReturnError(expectedErr)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	_, err = instance.Begin(context.Background(), postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLCommitError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	expectedErr := errors.New("cannot commit")

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(expectedErr)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	session, err := instance.Begin(context.Background(), postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
	if err != nil {
		t.Fatal(err)
	}

	err = session.Commit()
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLSegmentExecError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "INSERT INTO users (id, name) VALUES (\\$1, \\$2)"

	expectedErr := errors.New("cannot exec")

	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(1, "test").WillReturnError(expectedErr)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = session.Builder()(query).Arguments(1, "test").Exec()
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLSegmentQueryRowError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "SELECT id, name FROM users WHERE id = \\$1"

	expectedErr := errors.New("cannot query row")

	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(1).WillReturnError(expectedErr)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLSegmentQueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users"
	)

	expectedErr := errors.New("cannot query")

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnError(expectedErr)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = session.Builder()(query).Arguments(id, name).Query(func(r postgres.Rows) error {
		return nil
	})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLSegmentExecErrorRowsAffected(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "INSERT INTO users (id, name) VALUES (\\$1, \\$2)"

	expectedErr := errors.New("cannot exec")

	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(1, "test").WillReturnResult(sqlmock.NewErrorResult(expectedErr))

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = session.Builder()(query).Arguments(1, "test").Exec()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSQLSegmentExecTxErrorRowsAffected(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	query := "INSERT INTO users (id, name) VALUES (\\$1, \\$2)"

	expectedErr := errors.New("cannot exec")

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(1, "test").WillReturnResult(sqlmock.NewErrorResult(expectedErr))

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background(), postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))
	if err != nil {
		t.Fatal(err)
	}

	_, err = session.Builder()(query).Arguments(1, "test").Exec()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSQLWatchRollback(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	expectedErr := errors.New("some error")

	mock.ExpectBegin()
	mock.ExpectRollback()

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}

	err = instance.StartTransaction(context.Background(), func(session octobe.BuilderSession[postgres.Builder]) error {
		return expectedErr
	}, postgres.WithSQLTxOptions(postgres.SQLTxOptions{}))

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSQLSegmentQueryCloseRowsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	var (
		id    = 1
		name  = "test"
		query = "SELECT id, name FROM users"
	)

	expectedErr := errors.New("cannot query")
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(id, name).CloseError(expectedErr)

	mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)

	open := postgres.OpenWithConn(db)
	instance, err := octobe.New(open)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	session, err := instance.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = session.Builder()(query).Query(func(r postgres.Rows) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
