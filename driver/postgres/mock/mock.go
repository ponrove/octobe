package mock

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// expectation is an interface for different kinds of expectations.
type expectation interface {
	fulfilled() bool
	match(method string, args ...any) error
	getReturns() []any
	String() string
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

type PingExpectation struct {
	basicExpectation
}

func (e *PingExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

// ----------------------------------------------------------------------------
// Close
// ----------------------------------------------------------------------------

type CloseExpectation struct {
	basicExpectation
}

func (e *CloseExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

// ----------------------------------------------------------------------------
// Exec
// ----------------------------------------------------------------------------

// NewResult creates a new pgconn.CommandTag for Exec results.
func NewResult(command string, rowsAffected int64) pgconn.CommandTag {
	return pgconn.NewCommandTag(fmt.Sprintf("%s 0 %d", command, rowsAffected))
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

// ----------------------------------------------------------------------------
// Query
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// QueryRow
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

type BeginExpectation struct{ basicExpectation }

func (e *BeginExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

type BeginTxExpectation struct{ basicExpectation }

func (e *BeginTxExpectation) WithOptions(opts pgx.TxOptions) *BeginTxExpectation {
	e.args = []any{opts}
	return e
}

func (e *BeginTxExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

type CommitExpectation struct{ basicExpectation }

func (e *CommitExpectation) WillReturnError(err error) { e.returns = []any{err} }

type RollbackExpectation struct{ basicExpectation }

func (e *RollbackExpectation) WillReturnError(err error) { e.returns = []any{err} }

// ----------------------------------------------------------------------------
// Mock Row
// ----------------------------------------------------------------------------

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
