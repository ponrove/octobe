package mock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ponrove/octobe/driver/clickhouse"
)

var ErrNoExpectation = errors.New("no expectation found")

// Mock is a mock implementation of the clickhouse.NativeConn (driver.Conn) interface.
// It is designed to be used in tests to mock database interactions.
type Mock struct {
	mu           sync.Mutex
	expectations []expectation
	ordered      bool
}

var _ clickhouse.NativeConn = (*Mock)(nil)

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

func (m *Mock) Close() error {
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
	e.basicExpectation.WithArgs(args)
	return e
}

func (e *ExecExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

func (m *Mock) Exec(ctx context.Context, query string, args ...any) error {
	e, err := m.findExpectation("Exec", query, args)
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
	e.basicExpectation.WithArgs(args)
	return e
}

func (e *QueryExpectation) WillReturnRows(rows driver.Rows) {
	e.returns = []any{rows, nil}
}

func (e *QueryExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

func (m *Mock) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	e, err := m.findExpectation("Query", query, args)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return nil, ret[1].(error)
	}
	return ret[0].(driver.Rows), nil
}

// ----------------------------------------------------------------------------
// MockRows
// ----------------------------------------------------------------------------

type MockRows struct {
	columns []string
	rows    [][]any
	pos     int
	err     error
}

func NewMockRows(columns []string) *MockRows {
	return &MockRows{columns: columns}
}

func (r *MockRows) AddRow(values ...any) *MockRows {
	if len(values) != len(r.columns) {
		panic("number of values does not match number of columns")
	}
	r.rows = append(r.rows, values)
	return r
}

func (r *MockRows) Next() bool {
	r.pos++
	return r.pos <= len(r.rows)
}

func (r *MockRows) Scan(dest ...any) error {
	if r.pos > len(r.rows) {
		return io.EOF
	}
	if r.pos == 0 {
		return errors.New("scan called before next")
	}
	for i, val := range r.rows[r.pos-1] {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(val))
	}
	return nil
}

func (r *MockRows) Columns() []string                { return r.columns }
func (r *MockRows) Close() error                     { return nil }
func (r *MockRows) Err() error                       { return r.err }
func (r *MockRows) ScanStruct(dest any) error        { return errors.New("not implemented") }
func (r *MockRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *MockRows) Totals(...any) error              { return errors.New("not implemented") }
func (r *MockRows) NextResultSet() bool              { return false }

// GetRowsForTesting is a helper method for testing to get the raw rows data.
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
	e.basicExpectation.WithArgs(args)
	return e
}

func (e *QueryRowExpectation) WillReturnRow(row driver.Row) {
	e.returns = []any{row}
}

func (m *Mock) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	e, err := m.findExpectation("QueryRow", query, args)
	if err != nil {
		return &MockRow{err: err}
	}
	ret := e.getReturns()
	return ret[0].(driver.Row)
}

type MockRow struct {
	row []any
	err error
}

func NewMockRow(row ...any) *MockRow {
	return &MockRow{row: row}
}

// WillReturnError sets an error to be returned by Scan.
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

func (r *MockRow) ScanStruct(dest any) error { return errors.New("not implemented") }
func (r *MockRow) Err() error                { return r.err }

// ----------------------------------------------------------------------------
// Not implemented methods
// ----------------------------------------------------------------------------

func (m *Mock) Contributors() []string {
	panic("not implemented")
}

func (m *Mock) ServerVersion() (*driver.ServerVersion, error) {
	panic("not implemented")
}

func (m *Mock) Select(ctx context.Context, dest any, query string, args ...any) error {
	panic("not implemented")
}

func (m *Mock) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	panic("not implemented")
}

func (m *Mock) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	panic("not implemented")
}

func (m *Mock) Stats() driver.Stats {
	panic("not implemented")
}

// MockBatch is a mock for driver.Batch.
type MockBatch struct{}

func (b *MockBatch) Abort() error                    { return nil }
func (b *MockBatch) Append(...any) error             { return nil }
func (b *MockBatch) AppendStruct(any) error          { return nil }
func (b *MockBatch) IsSent() bool                    { return false }
func (b *MockBatch) Send() error                     { return nil }
func (b *MockBatch) Rows() int                       { return 0 }
func (b *MockBatch) Flush() error                    { return nil }
func (b *MockBatch) Columns() []column.Interface     { return nil }
func (b *MockBatch) Column(i int) driver.BatchColumn { return nil }
func (b *MockBatch) Close() error                    { return nil }
