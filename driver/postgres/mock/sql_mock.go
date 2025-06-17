package mock

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/ponrove/octobe/driver/postgres"
)

// SQLMock is a mock implementation of the postgres.SQL interface.
// It is designed to be used in tests to mock database interactions.
//
// NOTE: Due to the design of `database/sql`, which returns concrete types
// like `*sql.Rows` and `*sql.Row` instead of interfaces, mocking it
// without a custom driver (like go-sqlmock) is limited. This implementation
// will panic for methods that return these types if mock data is expected.
type SQLMock struct {
	mu           sync.Mutex
	expectations []expectation
	ordered      bool
}

var _ postgres.SQL = (*SQLMock)(nil)

// NewSQLMock creates a new mock database connection.
func NewSQLMock() *SQLMock {
	return &SQLMock{}
}

func (m *SQLMock) findExpectation(method string, args ...any) (expectation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// find the first unfulfilled expectation that matches
	for _, e := range m.expectations {
		if e.fulfilled() {
			continue
		}
		if err := e.match(method, args...); err == nil {
			return e, nil
		}
	}

	return nil, fmt.Errorf("%w for %s with args %v", ErrNoExpectation, method, args)
}

// AllExpectationsMet checks if all expectations were met.
func (m *SQLMock) AllExpectationsMet() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.expectations {
		if !e.fulfilled() {
			return fmt.Errorf("unfulfilled expectation: %s", e)
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// Ping
// ----------------------------------------------------------------------------

func (m *SQLMock) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "PingContext"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *SQLMock) PingContext(ctx context.Context) error {
	e, err := m.findExpectation("PingContext")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Close
// ----------------------------------------------------------------------------

func (m *SQLMock) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *SQLMock) Close() error {
	e, err := m.findExpectation("Close")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Exec
// ----------------------------------------------------------------------------

type sqlResult struct {
	lastInsertID int64
	rowsAffected int64
	err          error
}

func (r *sqlResult) LastInsertId() (int64, error) {
	return r.lastInsertID, r.err
}

func (r *sqlResult) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}

// NewSQLResult creates a new sql.Result for Exec results.
func NewSQLResult(lastInsertID, rowsAffected int64) driver.Result {
	return &sqlResult{lastInsertID: lastInsertID, rowsAffected: rowsAffected}
}

func (m *SQLMock) ExpectExec(query string) *SQLExecExpectation {
	e := &SQLExecExpectation{
		basicExpectation: basicExpectation{
			method: "ExecContext",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

type SQLExecExpectation struct {
	basicExpectation
}

func (e *SQLExecExpectation) WithArgs(args ...any) *SQLExecExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *SQLExecExpectation) WillReturnResult(res sql.Result) {
	e.returns = []any{res, nil}
}

func (e *SQLExecExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

func (m *SQLMock) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	e, err := m.findExpectation("ExecContext", append([]any{query}, args...)...)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return nil, ret[1].(error)
	}
	return ret[0].(sql.Result), nil
}

func (m *SQLMock) Exec(query string, args ...any) (sql.Result, error) {
	return m.ExecContext(context.Background(), query, args...)
}

// ----------------------------------------------------------------------------
// Query
// ----------------------------------------------------------------------------

func (m *SQLMock) ExpectQuery(query string) *QueryExpectation {
	e := &QueryExpectation{
		basicExpectation: basicExpectation{
			method: "QueryContext",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *SQLMock) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	e, err := m.findExpectation("QueryContext", append([]any{query}, args...)...)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return nil, ret[1].(error)
	}

	if ret[0] != nil {
		// We can't create a mock *sql.Rows. This will only work if the user somehow
		// provides a real *sql.Rows, which is unlikely.
		return ret[0].(*sql.Rows), nil
	}

	panic("cannot provide mock *sql.Rows without a mock driver. Consider using go-sqlmock.")
}

func (m *SQLMock) Query(query string, args ...any) (*sql.Rows, error) {
	return m.QueryContext(context.Background(), query, args...)
}

// ----------------------------------------------------------------------------
// QueryRow
// ----------------------------------------------------------------------------

func (m *SQLMock) ExpectQueryRow(query string) *SQLQueryRowExpectation {
	e := &SQLQueryRowExpectation{
		basicExpectation: basicExpectation{
			method: "QueryRowContext",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

type SQLQueryRowExpectation struct {
	basicExpectation
}

func (e *SQLQueryRowExpectation) WithArgs(args ...any) *SQLQueryRowExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *SQLQueryRowExpectation) WillReturnRow(row *sql.Row) {
	e.returns = []any{row}
}

func (m *SQLMock) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	e, err := m.findExpectation("QueryRowContext", append([]any{query}, args...)...)
	if err != nil {
		// It's not possible to return an error from QueryRowContext directly.
		// The error is part of the returned *sql.Row. We can't create one with an error.
		panic(fmt.Sprintf("cannot return error for QueryRow: %s", err))
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(*sql.Row)
	}

	panic("cannot provide mock *sql.Row without a mock driver. Consider using go-sqlmock.")
}

func (m *SQLMock) QueryRow(query string, args ...any) *sql.Row {
	return m.QueryRowContext(context.Background(), query, args...)
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

func (m *SQLMock) ExpectBegin() *SQLBeginExpectation {
	e := &SQLBeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

type SQLBeginExpectation struct{ basicExpectation }

func (e *SQLBeginExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

func (m *SQLMock) Begin() (*sql.Tx, error) {
	panic("mocking transactions for database/sql is not supported without a mock driver")
}

func (m *SQLMock) ExpectBeginTx() *SQLBeginTxExpectation {
	e := &SQLBeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *SQLMock) BeginTx(ctx context.Context, txOptions *sql.TxOptions) (*sql.Tx, error) {
	e, err := m.findExpectation("BeginTx", txOptions)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if len(ret) > 1 && ret[1] != nil {
		return nil, ret[1].(error)
	}
	return &sql.Tx{}, nil
}

func (m *SQLMock) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *SQLMock) Commit(ctx context.Context) error {
	e, err := m.findExpectation("Commit")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

func (m *SQLMock) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *SQLMock) Rollback(ctx context.Context) error {
	e, err := m.findExpectation("Rollback")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Not implemented methods
// ----------------------------------------------------------------------------

func (m *SQLMock) SetConnMaxLifetime(d time.Duration) {
	panic("not implemented")
}

func (m *SQLMock) SetMaxIdleConns(n int) {
	panic("not implemented")
}

func (m *SQLMock) SetMaxOpenConns(n int) {
	panic("not implemented")
}

func (m *SQLMock) Stats() sql.DBStats {
	panic("not implemented")
}

func (m *SQLMock) Prepare(query string) (*sql.Stmt, error) {
	panic("not implemented")
}

func (m *SQLMock) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	panic("not implemented")
}
