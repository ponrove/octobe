package mock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ponrove/octobe/driver/postgres"
)

var ErrNoExpectation = errors.New("no expectation found")

// Mock is a mock implementation of the postgres.PGXConn interface.
// It is designed to be used in tests to mock database interactions.
type Mock struct {
	mu           sync.Mutex
	expectations []expectation
	ordered      bool
}

var (
	_ postgres.PGXConn = (*Mock)(nil)
	_ pgx.Tx           = (*Mock)(nil)
)

// NewMock creates a new mock connection.
func NewMock() *Mock {
	return &Mock{}
}

// expectation is an interface for different kinds of expectations.
type expectation interface {
	fulfilled() bool
	match(method string, args ...any) error
	getReturns() []any
	String() string
}

func (m *Mock) findExpectation(method string, args ...any) (expectation, error) {
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
func (m *Mock) AllExpectationsMet() error {
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
// Generic Expectation
// ----------------------------------------------------------------------------

type basicExpectation struct {
	method      string
	isFulfilled bool
	returns     []any
	query       *regexp.Regexp
	args        []any
}

func (e *basicExpectation) fulfilled() bool {
	return e.isFulfilled
}

func (e *basicExpectation) getReturns() []any {
	e.isFulfilled = true
	return e.returns
}

func (e *basicExpectation) WithArgs(args ...any) {
	e.args = args
}

func (e *basicExpectation) match(method string, args ...any) error {
	if e.method != method {
		return fmt.Errorf("method mismatch: expected %s, got %s", e.method, method)
	}

	if e.query != nil {
		query, ok := args[0].(string)
		if !ok {
			return fmt.Errorf("first argument was not a string query")
		}
		if !e.query.MatchString(query) {
			return fmt.Errorf("query does not match regexp %s", e.query)
		}
		args = args[1:]
	}

	if e.args != nil {
		if !reflect.DeepEqual(e.args, args) {
			return fmt.Errorf("args mismatch: expected %v, got %v", e.args, args)
		}
	}

	return nil
}

func (e *basicExpectation) String() string {
	return fmt.Sprintf("method %s with query %s and args %v", e.method, e.query, e.args)
}

// ----------------------------------------------------------------------------
// Ping
// ----------------------------------------------------------------------------

func (m *Mock) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "Ping"}}
	m.expectations = append(m.expectations, e)
	return e
}

type PingExpectation struct {
	basicExpectation
}

func (e *PingExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

func (m *Mock) Ping(ctx context.Context) error {
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

func (m *Mock) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

type CloseExpectation struct {
	basicExpectation
}

func (e *CloseExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

func (m *Mock) Close(ctx context.Context) error {
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

// NewResult creates a new pgconn.CommandTag for Exec results.
func NewResult(command string, rowsAffected int64) pgconn.CommandTag {
	return pgconn.NewCommandTag(fmt.Sprintf("%s 0 %d", command, rowsAffected))
}

func (m *Mock) ExpectExec(query string) *ExecExpectation {
	e := &ExecExpectation{
		basicExpectation: basicExpectation{
			method: "Exec",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

type ExecExpectation struct {
	basicExpectation
}

func (e *ExecExpectation) WithArgs(args ...any) *ExecExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *ExecExpectation) WillReturnResult(res pgconn.CommandTag) {
	e.returns = []any{res, nil}
}

func (e *ExecExpectation) WillReturnError(err error) {
	e.returns = []any{pgconn.CommandTag{}, err}
}

func (m *Mock) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
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

func (m *Mock) ExpectQuery(query string) *QueryExpectation {
	e := &QueryExpectation{
		basicExpectation: basicExpectation{
			method: "Query",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

type QueryExpectation struct {
	basicExpectation
}

func (e *QueryExpectation) WithArgs(args ...any) *QueryExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *QueryExpectation) WillReturnRows(rows pgx.Rows) {
	e.returns = []any{rows, nil}
}

func (e *QueryExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

func (m *Mock) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
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
// MockRows
// ----------------------------------------------------------------------------

type MockRows struct {
	fields []pgconn.FieldDescription
	rows   [][]any
	pos    int
	err    error
	closed bool
}

func NewMockRows(columns []string) *MockRows {
	fields := make([]pgconn.FieldDescription, len(columns))
	for i, col := range columns {
		fields[i] = pgconn.FieldDescription{Name: col}
	}
	return &MockRows{fields: fields, pos: -1}
}

func (r *MockRows) AddRow(values ...any) *MockRows {
	if len(values) != len(r.fields) {
		panic("number of values does not match number of columns")
	}
	r.rows = append(r.rows, values)
	return r
}

func (r *MockRows) Close() { r.closed = true }

func (r *MockRows) Err() error { return r.err }

func (r *MockRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *MockRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }

func (r *MockRows) Next() bool {
	if r.closed {
		return false
	}
	r.pos++
	return r.pos < len(r.rows)
}

func (r *MockRows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.closed {
		return errors.New("rows is closed")
	}
	if r.pos < 0 || r.pos >= len(r.rows) {
		return io.EOF
	}
	for i, val := range r.rows[r.pos] {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(val))
	}
	return nil
}

func (r *MockRows) Values() ([]any, error) {
	if r.pos < 0 || r.pos >= len(r.rows) {
		return nil, io.EOF
	}
	return r.rows[r.pos], nil
}

func (r *MockRows) RawValues() [][]byte {
	panic("not implemented")
}

func (r *MockRows) Conn() *pgx.Conn { return nil }

func (r *MockRows) GetRowsForTesting() [][]any {
	return r.rows
}

// ----------------------------------------------------------------------------
// QueryRow
// ----------------------------------------------------------------------------

func (m *Mock) ExpectQueryRow(query string) *QueryRowExpectation {
	e := &QueryRowExpectation{
		basicExpectation: basicExpectation{
			method: "QueryRow",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

type QueryRowExpectation struct {
	basicExpectation
}

func (e *QueryRowExpectation) WithArgs(args ...any) *QueryRowExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *QueryRowExpectation) WillReturnRow(row pgx.Row) {
	e.returns = []any{row}
}

func (m *Mock) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e, err := m.findExpectation("QueryRow", append([]any{query}, args...)...)
	if err != nil {
		return &MockRow{err: err}
	}
	ret := e.getReturns()
	return ret[0].(pgx.Row)
}

type MockRow struct {
	row []any
	err error
}

func NewMockRow(row ...any) *MockRow {
	return &MockRow{row: row}
}

func (r *MockRow) WillReturnError(err error) *MockRow {
	r.err = err
	return r
}

func (r *MockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, val := range r.row {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(val))
	}
	return nil
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

func (m *Mock) ExpectBegin() *BeginExpectation {
	e := &BeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

type BeginExpectation struct{ basicExpectation }

func (e *BeginExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

func (m *Mock) Begin(ctx context.Context) (pgx.Tx, error) {
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

func (m *Mock) ExpectBeginTx() *BeginTxExpectation {
	e := &BeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	m.expectations = append(m.expectations, e)
	return e
}

type BeginTxExpectation struct{ basicExpectation }

func (e *BeginTxExpectation) WithOptions(opts pgx.TxOptions) *BeginTxExpectation {
	e.args = []any{opts}
	return e
}

func (e *BeginTxExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

func (m *Mock) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
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

func (m *Mock) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

type CommitExpectation struct{ basicExpectation }

func (e *CommitExpectation) WillReturnError(err error) { e.returns = []any{err} }

func (m *Mock) Commit(ctx context.Context) error {
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

func (m *Mock) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

type RollbackExpectation struct{ basicExpectation }

func (e *RollbackExpectation) WillReturnError(err error) { e.returns = []any{err} }

func (m *Mock) Rollback(ctx context.Context) error {
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

func (m *Mock) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("not implemented")
}
func (m *Mock) Deallocate(context.Context, string) error { panic("not implemented") }
func (m *Mock) DeallocateAll(context.Context) error      { panic("not implemented") }
func (m *Mock) PgConn() *pgconn.PgConn                   { panic("not implemented") }
func (m *Mock) Config() *pgx.ConnConfig                  { panic("not implemented") }
func (m *Mock) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("not implemented")
}

func (m *Mock) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("not implemented")
}
func (m *Mock) LargeObjects() pgx.LargeObjects { panic("not implemented") }
func (m *Mock) Conn() *pgx.Conn                { panic("not implemented") }
