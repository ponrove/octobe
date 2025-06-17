package mock

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"

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

func (e *QueryExpectation) WillReturnRows(rows *MockRows) {
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

func (e *QueryRowExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

type BeginExpectation struct{ basicExpectation }

func (e *BeginExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

type PGXBeginTxExpectation struct{ basicExpectation }

func (e *PGXBeginTxExpectation) WithOptions(opts pgx.TxOptions) *PGXBeginTxExpectation {
	e.args = []any{opts}
	return e
}

func (e *PGXBeginTxExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

type CommitExpectation struct{ basicExpectation }

func (e *CommitExpectation) WillReturnError(err error) { e.returns = []any{err} }

type RollbackExpectation struct{ basicExpectation }

func (e *RollbackExpectation) WillReturnError(err error) { e.returns = []any{err} }

type SQLBeginTxExpectation struct{ basicExpectation }

func (e *SQLBeginTxExpectation) WithOptions(opts sql.TxOptions) *SQLBeginTxExpectation {
	e.args = []any{opts}
	return e
}

func (e *SQLBeginTxExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

// ----------------------------------------------------------------------------
// Conversion Helper
// ----------------------------------------------------------------------------

func assign(dest, src any) error {
	dst := reflect.ValueOf(dest)
	if dst.Kind() != reflect.Ptr {
		return errors.New("destination must be a pointer")
	}
	dst = dst.Elem()

	if src == nil {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	srcVal := reflect.ValueOf(src)

	// Direct assignment
	if srcVal.Type().AssignableTo(dst.Type()) {
		dst.Set(srcVal)
		return nil
	}

	// Conversion
	if srcVal.Type().ConvertibleTo(dst.Type()) {
		dst.Set(srcVal.Convert(dst.Type()))
		return nil
	}

	// String/[]byte to something else
	switch s := src.(type) {
	case string:
		switch dst.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			i, err := strconv.ParseInt(s, 10, dst.Type().Bits())
			if err != nil {
				return fmt.Errorf("could not convert string %q to int: %v", s, err)
			}
			dst.SetInt(i)
			return nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			u, err := strconv.ParseUint(s, 10, dst.Type().Bits())
			if err != nil {
				return fmt.Errorf("could not convert string %q to uint: %v", s, err)
			}
			dst.SetUint(u)
			return nil
		case reflect.Float32, reflect.Float64:
			f, err := strconv.ParseFloat(s, dst.Type().Bits())
			if err != nil {
				return fmt.Errorf("could not convert string %q to float: %v", s, err)
			}
			dst.SetFloat(f)
			return nil
		case reflect.Bool:
			b, err := strconv.ParseBool(s)
			if err != nil {
				return fmt.Errorf("could not convert string %q to bool: %v", s, err)
			}
			dst.SetBool(b)
			return nil
		}
	case []byte:
		return assign(dest, string(s))
	}

	return fmt.Errorf("can't scan value of type %T into %T", src, dest)
}

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
	if len(dest) != len(r.row) {
		return fmt.Errorf("destination count %d does not match row column count %d", len(dest), len(r.row))
	}
	for i, d := range dest {
		if err := assign(d, r.row[i]); err != nil {
			return err
		}
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
	if len(dest) != len(r.fields) {
		return fmt.Errorf("destination count %d does not match row column count %d", len(dest), len(r.fields))
	}
	for i, d := range dest {
		if err := assign(d, r.rows[r.pos][i]); err != nil {
			return err
		}
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
