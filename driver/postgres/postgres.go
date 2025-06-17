package postgres

import (
	"database/sql"
	"database/sql/driver"

	"github.com/jackc/pgx/v5"
	"github.com/ponrove/octobe"
)

// Driver is a type alias for octobe.Driver with specific types for conn, config, and Builder.
type PGXDriver octobe.Driver[pgxConn, pgxConfig, Builder]

// Driver is a type alias for octobe.Driver with specific types for conn, config, and Builder.
type SQLDriver octobe.Driver[sqlConn, sqlConfig, Builder]

// Builder is a function signature used for building queries with the pgx driver.
type Builder func(query string) Segment

// PGXTxOptions holds the options for a transaction.
type PGXTxOptions pgx.TxOptions

// Transaction isolation levels
const (
	Serializable    pgx.TxIsoLevel = pgx.Serializable
	RepeatableRead  pgx.TxIsoLevel = pgx.RepeatableRead
	ReadCommitted   pgx.TxIsoLevel = pgx.ReadCommitted
	ReadUncommitted pgx.TxIsoLevel = pgx.ReadUncommitted
)

// Transaction access modes
const (
	ReadWrite pgx.TxAccessMode = pgx.ReadWrite
	ReadOnly  pgx.TxAccessMode = pgx.ReadOnly
)

// Transaction deferrable modes
const (
	Deferrable    pgx.TxDeferrableMode = pgx.Deferrable
	NotDeferrable pgx.TxDeferrableMode = pgx.NotDeferrable
)

// SQLTxOptions holds the options for a transaction in the sql driver.
type SQLTxOptions driver.TxOptions

// pgxConfig defines various configurations possible for the pgx driver.
type pgxConfig struct {
	txOptions *PGXTxOptions
}

// sqlConfig defines various configurations possible for the sql driver.
type sqlConfig struct {
	txOptions *SQLTxOptions
}

// WithTransaction enables the use of a transaction for the session.
func WithPGXTxOptions(options PGXTxOptions) octobe.Option[pgxConfig] {
	return func(c *pgxConfig) {
		c.txOptions = &options
	}
}

// WithTransaction enables the use of a transaction for the session.
func WithSQLTxOptions(options SQLTxOptions) octobe.Option[sqlConfig] {
	return func(c *sqlConfig) {
		c.txOptions = &options
	}
}

// Handler is a signature type for a handler. The handler receives a builder of the specific driver and returns a result and an error.
type Handler[RESULT any] func(Builder) (RESULT, error)

// Execute executes a handler with a session builder, injecting the builder of the driver into the handler.
func Execute[RESULT any](session octobe.BuilderSession[Builder], f Handler[RESULT]) (RESULT, error) {
	return f(session.Builder())
}

// PGXSegment is an interface that represents a specific query that can be run only once. It keeps track of the query,
// arguments, and execution state.
type Segment interface {
	Arguments(args ...any) Segment
	Exec() (ExecResult, error)
	QueryRow(dest ...any) error
	Query(cb func(driver.Rows) error) error
}

// ExecResult is a struct that holds the result of an execution, specifically the number of rows affected by the query.
type ExecResult struct {
	RowsAffected int64
}

// Rows is an interface that represents a set of rows returned by a query. It provides methods to iterate over the rows
// and read their values. It is compatible with both pgx.Rows and sql.Rows types, allowing for flexibility in handling
// database results.
type Rows interface {
	// Err returns any error that occurred while reading. Err must only be called after the Rows is closed (either by
	// calling Close or by Next returning false). If it is called early it may return nil even if there was an error
	// executing the query.
	Err() error

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available or a fatal error has occurred.
	// It automatically closes rows when all rows are read.
	//
	// Callers should check rows.Err() after rows.Next() returns false to detect
	// whether result-set reading ended prematurely due to an error. See
	// Conn.Query for details.
	//
	// For simpler error handling, consider using the higher-level pgx v5
	// CollectRows() and ForEachRow() helpers instead.
	Next() bool

	// Scan reads the values from the current row into dest values positionally.
	// dest can include pointers to core types, values implementing the Scanner
	// interface, and nil. nil will skip the value entirely. It is an error to
	// call Scan without first calling Next() and checking that it returned true.
	Scan(dest ...any) error
}

var (
	_ Rows = (pgx.Rows)(nil)
	_ Rows = (*sql.Rows)(nil)
)
