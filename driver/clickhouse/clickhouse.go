package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ponrove/octobe"
)

// Driver is a type alias for octobe.Driver with specific types for conn, config, and Builder.
type Driver octobe.Driver[nativeConn, config, Builder]

// Builder is a function signature used for building queries with the clickhouse driver.
type Builder func(query string) Segment

// config defines various configurations possible for the native driver.
type config struct{}

// Handler is a signature type for a handler. The handler receives a builder of the specific driver and returns a result and an error.
type Handler[RESULT any] func(Builder) (RESULT, error)

// Execute executes a handler with a session builder, injecting the builder of the driver into the handler.
func Execute[RESULT any](session octobe.BuilderSession[Builder], f Handler[RESULT]) (RESULT, error) {
	return f(session.Builder())
}

// Segment is an interface that represents a specific query that can be run only once. It keeps track of the query,
// arguments, and execution state.
type Segment interface {
	Contributors() []string
	ServerVersion() (*ServerVersion, error)
	Select(dest any) error
	Arguments(args ...any) Segment
	Exec() error
	Query(cb func(Rows) error) error
	QueryRow(dest ...any) error
	PrepareBatch(opts ...PrepareBatchOption) (Batch, error)
	AsyncInsert(wait bool, args ...any) error
}

// ExecResult is a struct that holds the result of an execution, such as the number of rows affected.
type Rows = driver.Rows

// Row is a struct that holds the result of a single row query.
type Row = driver.Row

// ServerVersion is a struct that holds the version of the ClickHouse server.
type ServerVersion = driver.ServerVersion

// PrepareBatchOption is a function signature that allows setting options for preparing a batch.
type PrepareBatchOption = driver.PrepareBatchOption

// Batch is a type that represents a batch of queries to be executed together.
type Batch = driver.Batch
