package postgres

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ponrove/octobe"
)

// Builder is a function signature used for building queries with the pgx driver.
type Builder func(query string) Segment

// TxOptions holds the options for a transaction.
type TxOptions pgx.TxOptions

// config defines various configurations possible for the pgx driver.
type config struct {
	txOptions *TxOptions
}

// WithTransaction enables the use of a transaction for the session.
func WithTransaction(options TxOptions) octobe.Option[config] {
	return func(c *config) {
		c.txOptions = &options
	}
}

// Handler is a signature type for a handler. The handler receives a builder of the specific driver and returns a result and an error.
type Handler[RESULT any] func(Builder) (RESULT, error)

// Execute executes a handler with a session builder, injecting the builder of the driver into the handler.
func Execute[RESULT any](session octobe.BuilderSession[Builder], f Handler[RESULT]) (RESULT, error) {
	return f(session.Builder())
}

// Segment is an interface that represents a specific query that can be run only once. It keeps track of the query,
// arguments, and execution state.
type Segment interface {
	Arguments(args ...any) Segment
	Exec() (pgconn.CommandTag, error)
	QueryRow(dest ...any) error
	Query(cb func(pgx.Rows) error) error
}
