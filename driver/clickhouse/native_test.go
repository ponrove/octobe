package clickhouse_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/clickhouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockNativeConn is a mock implementation of the clickhouse.NativeConn interface.
type mockNativeConn struct {
	mock.Mock
}

func (m *mockNativeConn) BeginTx(ctx context.Context, opts *clickhouse.TxOptions) (clickhouse.Tx, error) {
	args := m.Called(ctx, opts)
	if tx, ok := args.Get(0).(clickhouse.Tx); ok {
		return tx, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockNativeConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockNativeConn) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockNativeConn) Exec(ctx context.Context, query string, args ...any) error {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	calledArgs := m.Called(callArgs...)
	return calledArgs.Error(0)
}

func (m *mockNativeConn) Query(ctx context.Context, query string, args ...any) (clickhouse.Rows, error) {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	calledArgs := m.Called(callArgs...)
	if rows, ok := calledArgs.Get(0).(clickhouse.Rows); ok {
		return rows, calledArgs.Error(1)
	}
	return nil, calledArgs.Error(1)
}

func (m *mockNativeConn) QueryRow(ctx context.Context, query string, args ...any) clickhouse.Row {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	calledArgs := m.Called(callArgs...)
	return calledArgs.Get(0).(clickhouse.Row)
}

// mockTx is a mock implementation of the clickhouse.Tx interface.
type mockTx struct {
	mock.Mock
}

func (m *mockTx) Commit() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockTx) Rollback() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockTx) Exec(ctx context.Context, query string, args ...any) error {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	calledArgs := m.Called(callArgs...)
	return calledArgs.Error(0)
}

func (m *mockTx) Query(ctx context.Context, query string, args ...any) (clickhouse.Rows, error) {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	calledArgs := m.Called(callArgs...)
	if rows, ok := calledArgs.Get(0).(clickhouse.Rows); ok {
		return rows, calledArgs.Error(1)
	}
	return nil, calledArgs.Error(1)
}

func (m *mockTx) QueryRow(ctx context.Context, query string, args ...any) clickhouse.Row {
	callArgs := []interface{}{ctx, query}
	callArgs = append(callArgs, args...)
	calledArgs := m.Called(callArgs...)
	return calledArgs.Get(0).(clickhouse.Row)
}

// mockRow is a mock implementation of the clickhouse.Row interface.
type mockRow struct {
	mock.Mock
}

func (m *mockRow) Scan(dest ...any) error {
	args := m.Called(dest...)
	return args.Error(0)
}

func TestNativeWithTxInsideStartTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	tx := &mockTx{}
	row := &mockRow{}

	query := "SELECT id, name FROM users WHERE id = ?"
	conn.On("BeginTx", ctx, &clickhouse.TxOptions{}).Return(tx, nil)
	tx.On("QueryRow", ctx, query, 1).Return(row)
	row.On("Scan", mock.Anything).Return(nil)
	tx.On("Commit").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = instance.StartTransaction(ctx, func(session octobe.BuilderSession[clickhouse.Builder]) error {
		return session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	}, clickhouse.WithNativeTxOptions(clickhouse.NativeTxOptions{}))
	assert.NoError(t, err)

	conn.AssertExpectations(t)
	tx.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestNativeWithTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	tx := &mockTx{}
	row := &mockRow{}

	query := "SELECT id, name FROM users WHERE id = ?"
	conn.On("BeginTx", ctx, &clickhouse.TxOptions{}).Return(tx, nil)
	conn.On("Close").Return(nil)
	tx.On("QueryRow", ctx, query, 1).Return(row)
	row.On("Scan", mock.Anything).Return(nil)
	tx.On("Commit").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(ctx)

	session, err := instance.Begin(ctx, clickhouse.WithNativeTxOptions(clickhouse.NativeTxOptions{}))
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.NoError(t, err)

	assert.NoError(t, session.Commit())

	conn.AssertExpectations(t)
	tx.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestNativeWithoutTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	row := &mockRow{}

	query := "SELECT id, name FROM users WHERE id = ?"
	conn.On("QueryRow", ctx, query, 1).Return(row)
	conn.On("Close").Return(nil)
	row.On("Scan", mock.Anything).Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(ctx)

	session, err := instance.Begin(ctx)
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.NoError(t, err)

	conn.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestNativeWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	tx := &mockTx{}
	row := &mockRow{}
	expectedErr := errors.New("sql error")

	query := "SELECT id, name FROM users WHERE id = ?"
	conn.On("BeginTx", ctx, &clickhouse.TxOptions{}).Return(tx, nil)
	tx.On("QueryRow", ctx, query, 1).Return(row)
	row.On("Scan", mock.Anything).Return(expectedErr)
	tx.On("Rollback").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	err = instance.StartTransaction(ctx, func(session octobe.BuilderSession[clickhouse.Builder]) error {
		var (
			destID   int
			destName string
		)
		return session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	}, clickhouse.WithNativeTxOptions(clickhouse.NativeTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	conn.AssertExpectations(t)
	tx.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestNativeWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	tx := &mockTx{}

	conn.On("BeginTx", ctx, &clickhouse.TxOptions{}).Return(tx, nil)
	tx.On("Rollback").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	defer func() {
		assert.NotNil(t, recover(), "The code did not panic")
		conn.AssertExpectations(t)
		tx.AssertExpectations(t)
	}()

	_ = instance.StartTransaction(ctx, func(session octobe.BuilderSession[clickhouse.Builder]) error {
		panic("something went wrong")
	}, clickhouse.WithNativeTxOptions(clickhouse.NativeTxOptions{}))
}

func TestNativeWithTxManualRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	tx := &mockTx{}
	row := &mockRow{}
	expectedErr := errors.New("sql error")

	query := "SELECT id, name FROM users WHERE id = ?"
	conn.On("BeginTx", ctx, &clickhouse.TxOptions{}).Return(tx, nil)
	tx.On("QueryRow", ctx, query, 1).Return(row)
	row.On("Scan", mock.Anything).Return(expectedErr)
	tx.On("Rollback").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(ctx, clickhouse.WithNativeTxOptions(clickhouse.NativeTxOptions{}))
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.Error(t, err)

	assert.NoError(t, session.Rollback())
	conn.AssertExpectations(t)
	tx.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestNativeWithoutTxCommit(t *testing.T) {
	t.Parallel()

	conn := &mockNativeConn{}

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	err = session.Commit()
	assert.Error(t, err)
	assert.Equal(t, "cannot commit without transaction", err.Error())
}

func TestNativeWithoutTxRollback(t *testing.T) {
	t.Parallel()

	conn := &mockNativeConn{}

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(context.Background())
	assert.NoError(t, err)

	err = session.Rollback()
	assert.Error(t, err)
	assert.Equal(t, "cannot rollback without transaction", err.Error())
}

func TestNativeSegmentUsedTwice(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := &mockNativeConn{}
	row := &mockRow{}

	query := "SELECT id, name FROM users WHERE id = ?"
	conn.On("QueryRow", ctx, query, 1).Return(row)
	row.On("Scan", mock.Anything).Return(nil)
	conn.On("Close").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(ctx)

	session, err := instance.Begin(ctx)
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	q := session.Builder()(query).Arguments(1)
	err = q.QueryRow(&destID, &destName)
	assert.NoError(t, err)

	// This should return an error
	err = q.QueryRow(&destID, &destName)
	assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

	conn.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestOpenNativeWithConnNil(t *testing.T) {
	t.Parallel()

	open := clickhouse.OpenNativeWithConn(nil)
	_, err := octobe.New(open)
	assert.Error(t, err)
	assert.Equal(t, "conn is nil", err.Error())
}

func TestNativeBeginError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	expectedErr := errors.New("cannot begin")

	conn.On("BeginTx", ctx, &clickhouse.TxOptions{}).Return(nil, expectedErr)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	_, err = instance.Begin(ctx, clickhouse.WithNativeTxOptions(clickhouse.NativeTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	conn.AssertExpectations(t)
}

func TestNativeCommitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	tx := &mockTx{}
	expectedErr := errors.New("cannot commit")

	conn.On("BeginTx", ctx, &clickhouse.TxOptions{}).Return(tx, nil)
	tx.On("Commit").Return(expectedErr)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)

	session, err := instance.Begin(ctx, clickhouse.WithNativeTxOptions(clickhouse.NativeTxOptions{}))
	assert.NoError(t, err)

	err = session.Commit()
	assert.ErrorIs(t, err, expectederr)

	conn.AssertExpectations(t)
	tx.AssertExpectations(t)
}

func TestNativeSegmentExecError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	query := "INSERT INTO users (id, name) VALUES (?, ?)"
	expectedErr := errors.New("cannot exec")
	conn.On("Exec", ctx, query, 1, "test").Return(expectedErr)
	conn.On("Close").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(ctx)

	session, err := instance.Begin(ctx)
	assert.NoError(t, err)

	_, err = session.Builder()(query).Arguments(1, "test").Exec()
	assert.ErrorIs(t, err, expectedErr)

	conn.AssertExpectations(t)
}

func TestNativeSegmentQueryRowError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn := &mockNativeConn{}
	row := &mockRow{}
	query := "SELECT id, name FROM users WHERE id = ?"
	expectedErr := errors.New("cannot query row")
	conn.On("QueryRow", ctx, query, 1).Return(row)
	row.On("Scan", mock.Anything).Return(expectedErr)
	conn.On("Close").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(ctx)

	session, err := instance.Begin(ctx)
	assert.NoError(t, err)

	var (
		destID   int
		destName string
	)
	err = session.Builder()(query).Arguments(1).QueryRow(&destID, &destName)
	assert.ErrorIs(t, err, expectedErr)

	conn.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestNativeSegmentQueryError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := &mockNativeConn{}
	query := "SELECT id, name FROM users"
	expectedErr := errors.New("cannot query")
	conn.On("Query", ctx, query, mock.Anything).Return(nil, expectedErr)
	conn.On("Close").Return(nil)

	open := clickhouse.OpenNativeWithConn(conn)
	instance, err := octobe.New(open)
	assert.NoError(t, err)
	defer instance.Close(ctx)

	session, err := instance.Begin(ctx)
	assert.NoError(t, err)

	err = session.Builder()(query).Query(func(r clickhouse.Rows) error {
		return nil
	})

	assert.ErrorIs(t, err, expectedErr)
	conn.AssertExpectations(t)
}
