package clickhouse_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/clickhouse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockConn is a mock implementation of the clickhouse.NativeConn (driver.Conn) interface.
type MockConn struct {
	mock.Mock
}

func (m *MockConn) Contributors() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockConn) ServerVersion() (*driver.ServerVersion, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*driver.ServerVersion), args.Error(1)
}

func (m *MockConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	// To match []any{...} as an argument, we need to package our variadic args.
	calledArgs := m.Called(ctx, dest, query, args)
	return calledArgs.Error(0)
}

func (m *MockConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	calledArgs := m.Called(ctx, query, args)
	if calledArgs.Get(0) == nil {
		return nil, calledArgs.Error(1)
	}
	return calledArgs.Get(0).(driver.Rows), calledArgs.Error(1)
}

func (m *MockConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	calledArgs := m.Called(ctx, query, args)
	return calledArgs.Get(0).(driver.Row)
}

func (m *MockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	// To match variadic options, we need to package them.
	calledArgs := m.Called(ctx, query, opts)
	if calledArgs.Get(0) == nil {
		return nil, calledArgs.Error(1)
	}
	return calledArgs.Get(0).(driver.Batch), calledArgs.Error(1)
}

func (m *MockConn) Exec(ctx context.Context, query string, args ...any) error {
	calledArgs := m.Called(ctx, query, args)
	return calledArgs.Error(0)
}

func (m *MockConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	calledArgs := m.Called(ctx, query, wait, args)
	return calledArgs.Error(0)
}

func (m *MockConn) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockConn) Stats() driver.Stats {
	args := m.Called()
	if args.Get(0) == nil {
		return driver.Stats{}
	}
	return args.Get(0).(driver.Stats)
}

func (m *MockConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockRow is a mock implementation of the driver.Row interface.
type MockRow struct {
	mock.Mock
}

func (m *MockRow) Scan(dest ...any) error {
	// To match []any{...} as an argument, we need to package our variadic args.
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockRow) ScanStruct(dest any) error {
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockRow) Err() error {
	args := m.Called()
	return args.Error(0)
}

// MockRows is a mock implementation of the driver.Rows interface.
type MockRows struct {
	mock.Mock
}

func (m *MockRows) Next() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockRows) Scan(dest ...any) error {
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockRows) ScanStruct(dest any) error {
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockRows) ColumnTypes() []driver.ColumnType {
	args := m.Called()
	return args.Get(0).([]driver.ColumnType)
}

func (m *MockRows) Columns() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockRows) Totals(totals ...any) error {
	args := m.Called(totals)
	return args.Error(0)
}

func (m *MockRows) NextResultSet() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockRows) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockRows) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockBatch is a mock implementation of the driver.Batch interface.
type MockBatch struct {
	mock.Mock
}

func (m *MockBatch) Abort() error {
	return m.Called().Error(0)
}

func (m *MockBatch) Append(args ...any) error {
	return m.Called(args).Error(0)
}

func (m *MockBatch) AppendStruct(s any) error {
	return m.Called(s).Error(0)
}

func (m *MockBatch) IsSent() bool {
	return m.Called().Bool(0)
}

func (m *MockBatch) Send() error {
	return m.Called().Error(0)
}

func (m *MockBatch) Rows() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockBatch) Flush() error {
	return m.Called().Error(0)
}

func (m *MockBatch) Columns() []column.Interface {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]column.Interface)
}

func (m *MockBatch) Column(i int) driver.BatchColumn {
	args := m.Called(i)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(driver.BatchColumn)
}

func (m *MockBatch) Close() error {
	return m.Called().Error(0)
}

var _ driver.Batch = &MockBatch{}

func TestOpenWithConnNil(t *testing.T) {
	_, err := octobe.New(clickhouse.OpenNativeWithConn(nil))
	require.Error(t, err)
	require.Equal(t, "conn is nil", err.Error())
}

func TestClose(t *testing.T) {
	ctx := context.Background()
	mockConn := new(MockConn)
	o, err := octobe.New(clickhouse.OpenNativeWithConn(mockConn))
	require.NoError(t, err)

	mockConn.On("Close").Return(nil)
	err = o.Close(ctx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestPing(t *testing.T) {
	ctx := context.Background()
	mockConn := new(MockConn)
	o, err := octobe.New(clickhouse.OpenNativeWithConn(mockConn))
	require.NoError(t, err)

	mockConn.On("Ping", ctx).Return(nil)
	err = o.Ping(ctx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestCommitRollback(t *testing.T) {
	ctx := context.Background()
	mockConn := new(MockConn)
	o, err := octobe.New(clickhouse.OpenNativeWithConn(mockConn))
	require.NoError(t, err)

	session, err := o.Begin(ctx)
	require.NoError(t, err)

	// Commit and Rollback are no-ops for clickhouse, should not panic and return nil
	require.NoError(t, session.Commit())
	require.NoError(t, session.Rollback())
}

func TestSegmentUsedTwice(t *testing.T) {
	ctx := context.Background()
	query := "SELECT 1"
	var args []any

	// Common setup for all sub-tests
	setup := func(t *testing.T) (octobe.Session[clickhouse.Builder], *MockConn) {
		mockConn := new(MockConn)
		o, err := octobe.New(clickhouse.OpenNativeWithConn(mockConn))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)
		return session, mockConn
	}

	t.Run("Exec", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)

		mockConn.On("Exec", ctx, query, args).Return(nil).Once()
		require.NoError(t, s.Exec())

		// Second call
		require.Equal(t, octobe.ErrAlreadyUsed, s.Exec())
		mockConn.AssertExpectations(t)
	})

	t.Run("Select", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		var dest []struct{ N int }

		mockConn.On("Select", ctx, &dest, query, args).Return(nil).Once()
		require.NoError(t, s.Select(&dest))

		require.Equal(t, octobe.ErrAlreadyUsed, s.Select(&dest))
		mockConn.AssertExpectations(t)
	})

	t.Run("QueryRow", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		var dest int

		mockRow := new(MockRow)
		mockRow.On("Scan", []any{&dest}).Return(nil).Once()
		mockConn.On("QueryRow", ctx, query, args).Return(mockRow).Once()

		require.NoError(t, s.QueryRow(&dest))

		require.Equal(t, octobe.ErrAlreadyUsed, s.QueryRow(&dest))
		mockConn.AssertExpectations(t)
		mockRow.AssertExpectations(t)
	})

	t.Run("Query", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)

		mockRows := new(MockRows)
		mockRows.On("Close").Return(nil).Once()
		mockRows.On("Err").Return(nil).Once()
		mockConn.On("Query", ctx, query, args).Return(mockRows, nil).Once()

		cb := func(rows driver.Rows) error { return nil }
		require.NoError(t, s.Query(cb))

		require.Equal(t, octobe.ErrAlreadyUsed, s.Query(cb))
		mockConn.AssertExpectations(t)
		mockRows.AssertExpectations(t)
	})

	t.Run("PrepareBatch", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)

		mockBatch := new(MockBatch)
		mockConn.On("PrepareBatch", ctx, query, []driver.PrepareBatchOption(nil)).Return(mockBatch, nil).Once()

		_, err := s.PrepareBatch()
		require.NoError(t, err)

		_, err = s.PrepareBatch()
		require.Equal(t, octobe.ErrAlreadyUsed, err)
		mockConn.AssertExpectations(t)
	})

	t.Run("AsyncInsert", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)

		mockConn.On("AsyncInsert", ctx, query, true, args).Return(nil).Once()
		require.NoError(t, s.AsyncInsert(true))

		require.Equal(t, octobe.ErrAlreadyUsed, s.AsyncInsert(true))
		mockConn.AssertExpectations(t)
	})
}

func TestSegmentError(t *testing.T) {
	ctx := context.Background()
	query := "SELECT 1"
	var sArgs []any
	expectedErr := errors.New("something went wrong")

	setup := func(t *testing.T) (octobe.Session[clickhouse.Builder], *MockConn) {
		mockConn := new(MockConn)
		o, err := octobe.New(clickhouse.OpenNativeWithConn(mockConn))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)
		return session, mockConn
	}

	t.Run("Exec", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		mockConn.On("Exec", ctx, query, sArgs).Return(expectedErr)
		err := s.Exec()
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		mockConn.AssertExpectations(t)
	})

	t.Run("QueryRow", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		var dest int
		mockRow := new(MockRow)
		mockRow.On("Scan", []any{&dest}).Return(expectedErr)
		mockConn.On("QueryRow", ctx, query, sArgs).Return(mockRow)

		err := s.QueryRow(&dest)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		mockConn.AssertExpectations(t)
		mockRow.AssertExpectations(t)
	})

	t.Run("Query - initial error", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		mockConn.On("Query", ctx, query, sArgs).Return(nil, expectedErr)

		err := s.Query(func(rows driver.Rows) error { return nil })
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		mockConn.AssertExpectations(t)
	})

	t.Run("Query - callback error", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		mockRows := new(MockRows)
		mockRows.On("Close").Return(nil)
		mockConn.On("Query", ctx, query, sArgs).Return(mockRows, nil)

		err := s.Query(func(rows driver.Rows) error { return expectedErr })
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		mockConn.AssertExpectations(t)
		mockRows.AssertExpectations(t)
	})

	t.Run("Query - rows error", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		mockRows := new(MockRows)
		mockRows.On("Close").Return(nil)
		mockRows.On("Err").Return(expectedErr)
		mockConn.On("Query", ctx, query, sArgs).Return(mockRows, nil)

		err := s.Query(func(rows driver.Rows) error { return nil })
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		mockConn.AssertExpectations(t)
		mockRows.AssertExpectations(t)
	})
}

func TestHelpers(t *testing.T) {
	ctx := context.Background()
	query := "SELECT 1"

	setup := func(t *testing.T) (octobe.Session[clickhouse.Builder], *MockConn) {
		mockConn := new(MockConn)
		o, err := octobe.New(clickhouse.OpenNativeWithConn(mockConn))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)
		return session, mockConn
	}

	t.Run("Contributors", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		expected := []string{"test"}
		mockConn.On("Contributors").Return(expected)
		actual := s.Contributors()
		require.Equal(t, expected, actual)
		mockConn.AssertExpectations(t)
	})

	t.Run("ServerVersion", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		expected := &driver.ServerVersion{
			Name:        "",
			DisplayName: "",
			Revision:    1,
			Version: proto.Version{
				Major: 22,
				Minor: 3,
				Patch: 0,
			},
			Timezone: nil,
		}
		mockConn.On("ServerVersion").Return(expected, nil)
		actual, err := s.ServerVersion()
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		mockConn.AssertExpectations(t)
	})

	t.Run("Arguments", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		args := []any{1, "test"}
		s.Arguments(args...)

		mockConn.On("Exec", ctx, query, args).Return(nil)
		err := s.Exec()
		require.NoError(t, err)
		mockConn.AssertExpectations(t)
	})

	t.Run("AsyncInsert with arguments", func(t *testing.T) {
		session, mockConn := setup(t)
		s := session.Builder()(query)
		args := []any{1, "test"}

		mockConn.On("AsyncInsert", ctx, query, true, args).Return(nil)
		err := s.AsyncInsert(true, args...)
		require.NoError(t, err)
		mockConn.AssertExpectations(t)
	})
}

func TestStartTransaction(t *testing.T) {
	ctx := context.Background()
	mockConn := new(MockConn)
	o, err := octobe.New(clickhouse.OpenNativeWithConn(mockConn))
	require.NoError(t, err)

	handler := func(session octobe.BuilderSession[clickhouse.Builder]) error {
		s := session.Builder()("SELECT 1")
		mockConn.On("Exec", mock.Anything, "SELECT 1", mock.Anything).Return(nil).Once()
		return s.Exec()
	}

	t.Run("Success", func(t *testing.T) {
		err = o.StartTransaction(ctx, handler)
		require.NoError(t, err)
	})

	t.Run("Error", func(t *testing.T) {
		expectedErr := fmt.Errorf("handler error")
		err = o.StartTransaction(ctx, func(session octobe.BuilderSession[clickhouse.Builder]) error {
			return expectedErr
		})
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})

	t.Run("Panic", func(t *testing.T) {
		expectedPanic := "handler panic"
		require.PanicsWithValue(t, expectedPanic, func() {
			_ = o.StartTransaction(ctx, func(session octobe.BuilderSession[clickhouse.Builder]) error {
				panic(expectedPanic)
			})
		})
	})
}
