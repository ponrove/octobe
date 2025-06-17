package mock

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ponrove/octobe/driver/postgres"
)

// PGXPoolMock is a mock implementation of the postgres.PGXPool interface.
// It is designed to be used in tests to mock database interactions.
type PGXPoolMock struct {
	mu           sync.Mutex
	expectations []expectation
	ordered      bool
}

var (
	_ postgres.PGXPool = (*PGXPoolMock)(nil)
	_ pgx.Tx           = (*PGXPoolMock)(nil)
)

// NewPGXPoolMock creates a new mock connection pool.
func NewPGXPoolMock() *PGXPoolMock {
	return &PGXPoolMock{}
}

func (m *PGXPoolMock) findExpectation(method string, args ...any) (expectation, error) {
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
func (m *PGXPoolMock) AllExpectationsMet() error {
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

func (m *PGXPoolMock) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "Ping"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Ping(ctx context.Context) error {
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

func (m *PGXPoolMock) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Close() {
	e, err := m.findExpectation("Close")
	if err != nil {
		return
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return
	}
	return
}

// ----------------------------------------------------------------------------
// Exec
// ----------------------------------------------------------------------------

func (m *PGXPoolMock) ExpectExec(query string) *ExecExpectation {
	e := &ExecExpectation{
		basicExpectation: basicExpectation{
			method: "Exec",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
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

func (m *PGXPoolMock) ExpectQuery(query string) *QueryExpectation {
	e := &QueryExpectation{
		basicExpectation: basicExpectation{
			method: "Query",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
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

func (m *PGXPoolMock) ExpectQueryRow(query string) *QueryRowExpectation {
	e := &QueryRowExpectation{
		basicExpectation: basicExpectation{
			method: "QueryRow",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e, err := m.findExpectation("QueryRow", append([]any{query}, args...)...)
	if err != nil {
		return &MockRow{err: err}
	}
	ret := e.getReturns()
	if len(ret) >= 2 && ret[1] != nil {
		return &MockRow{err: ret[1].(error)}
	}
	if ret[0] == nil {
		return nil
	}
	return ret[0].(pgx.Row)
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

func (m *PGXPoolMock) ExpectBegin() *BeginExpectation {
	e := &BeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Begin(ctx context.Context) (pgx.Tx, error) {
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

func (m *PGXPoolMock) ExpectBeginTx(txOptions postgres.PGXTxOptions) *PGXBeginTxExpectation {
	e := &PGXBeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	e.WithOptions(pgx.TxOptions(txOptions))
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
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

func (m *PGXPoolMock) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Commit(ctx context.Context) error {
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

func (m *PGXPoolMock) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Rollback(ctx context.Context) error {
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

func (m *PGXPoolMock) Acquire(context.Context) (*pgxpool.Conn, error) {
	panic("not implemented")
}

func (m *PGXPoolMock) AcquireFunc(context.Context, func(*pgxpool.Conn) error) error {
	panic("not implemented")
}
func (m *PGXPoolMock) AcquireAllIdle(context.Context) []*pgxpool.Conn { panic("not implemented") }
func (m *PGXPoolMock) Reset()                                         { panic("not implemented") }
func (m *PGXPoolMock) Config() *pgxpool.Config                        { panic("not implemented") }
func (m *PGXPoolMock) Stat() *pgxpool.Stat                            { panic("not implemented") }
func (m *PGXPoolMock) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("not implemented")
}

func (m *PGXPoolMock) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("not implemented")
}
func (m *PGXPoolMock) LargeObjects() pgx.LargeObjects { panic("not implemented") }
func (m *PGXPoolMock) Conn() *pgx.Conn                { panic("not implemented") }
func (m *PGXPoolMock) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("not implemented")
}
