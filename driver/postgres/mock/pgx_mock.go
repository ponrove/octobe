package mock

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ponrove/octobe/driver/postgres"
)

var ErrNoExpectation = errors.New("no expectation found")

// PGXMock is a mock implementation of the postgres.PGXConn interface.
// It is designed to be used in tests to mock database interactions.
type PGXMock struct {
	mu           sync.Mutex
	expectations []expectation
	ordered      bool
}

var (
	_ postgres.PGXConn = (*PGXMock)(nil)
	_ pgx.Tx           = (*PGXMock)(nil)
)

// NewMock creates a new mock connection.
func NewMock() *PGXMock {
	return &PGXMock{}
}

func (m *PGXMock) findExpectation(method string, args ...any) (expectation, error) {
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
func (m *PGXMock) AllExpectationsMet() error {
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

func (m *PGXMock) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "Ping"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Ping(ctx context.Context) error {
	e, err := m.findExpectation("Ping")
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

func (m *PGXMock) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Close(ctx context.Context) error {
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

func (m *PGXMock) ExpectExec(query string) *ExecExpectation {
	e := &ExecExpectation{
		basicExpectation: basicExpectation{
			method: "Exec",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	e, err := m.findExpectation("Exec", append([]any{query}, args...)...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return pgconn.CommandTag{}, ret[1].(error)
	}
	return ret[0].(pgconn.CommandTag), nil
}

// ----------------------------------------------------------------------------
// Query
// ----------------------------------------------------------------------------

func (m *PGXMock) ExpectQuery(query string) *QueryExpectation {
	e := &QueryExpectation{
		basicExpectation: basicExpectation{
			method: "Query",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	e, err := m.findExpectation("Query", append([]any{query}, args...)...)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return nil, ret[1].(error)
	}
	if ret[0] == nil {
		return nil, nil
	}
	return ret[0].(pgx.Rows), nil
}

// ----------------------------------------------------------------------------
// QueryRow
// ----------------------------------------------------------------------------

func (m *PGXMock) ExpectQueryRow(query string) *QueryRowExpectation {
	e := &QueryRowExpectation{
		basicExpectation: basicExpectation{
			method: "QueryRow",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e, err := m.findExpectation("QueryRow", append([]any{query}, args...)...)
	if err != nil {
		return &MockRow{err: err}
	}
	ret := e.getReturns()
	return ret[0].(pgx.Row)
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

func (m *PGXMock) ExpectBegin() *BeginExpectation {
	e := &BeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Begin(ctx context.Context) (pgx.Tx, error) {
	e, err := m.findExpectation("Begin")
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if len(ret) > 1 && ret[1] != nil {
		return nil, ret[1].(error)
	}
	return m, nil
}

func (m *PGXMock) ExpectBeginTx() *BeginTxExpectation {
	e := &BeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	e, err := m.findExpectation("BeginTx", txOptions)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if len(ret) > 1 && ret[1] != nil {
		return nil, ret[1].(error)
	}
	return m, nil
}

func (m *PGXMock) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Commit(ctx context.Context) error {
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

func (m *PGXMock) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Rollback(ctx context.Context) error {
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

func (m *PGXMock) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("not implemented")
}
func (m *PGXMock) Deallocate(context.Context, string) error { panic("not implemented") }
func (m *PGXMock) DeallocateAll(context.Context) error      { panic("not implemented") }
func (m *PGXMock) PgConn() *pgconn.PgConn                   { panic("not implemented") }
func (m *PGXMock) Config() *pgx.ConnConfig                  { panic("not implemented") }
func (m *PGXMock) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("not implemented")
}

func (m *PGXMock) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("not implemented")
}
func (m *PGXMock) LargeObjects() pgx.LargeObjects { panic("not implemented") }
func (m *PGXMock) Conn() *pgx.Conn                { panic("not implemented") }
