package clickhouse

import (
	"context"
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ponrove/octobe"
)

type NativeConn = driver.Conn

// nativeConn holds the connection and default configuration for the native driver.
type nativeConn struct {
	conn NativeConn
}

// Ensure nativeConn implements the octobe.Driver interface.
var _ octobe.Driver[nativeConn, config, Builder] = &nativeConn{}

// OpenNative creates a new database connection and returns a driver with the specified types.
func OpenNative(opts *clickhouse.Options) octobe.Open[nativeConn, config, Builder] {
	return func() (octobe.Driver[nativeConn, config, Builder], error) {
		conn, err := clickhouse.Open(opts)
		if err != nil {
			return nil, err
		}

		return &nativeConn{
			conn: conn,
		}, nil
	}
}

// OpenNativeWithConn creates a new database connection using an existing connection.
func OpenNativeWithConn(c NativeConn) octobe.Open[nativeConn, config, Builder] {
	return func() (octobe.Driver[nativeConn, config, Builder], error) {
		if c == nil {
			return nil, errors.New("conn is nil")
		}

		return &nativeConn{
			conn: c,
		}, nil
	}
}

// Begin starts a new session with the database and returns a Session instance.
func (d *nativeConn) Begin(ctx context.Context, opts ...octobe.Option[config]) (octobe.Session[Builder], error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	return &nativeSession{
		ctx: ctx,
		cfg: cfg,
		d:   d,
	}, nil
}

// Close closes the database connection.
func (d *nativeConn) Close(_ context.Context) error {
	return d.conn.Close()
}

// Ping checks the connection to the database to ensure it is still alive.
func (d *nativeConn) Ping(ctx context.Context) error {
	return d.conn.Ping(ctx)
}

// nativeSession holds nativeSession context, representing a series of related queries.
type nativeSession struct {
	ctx       context.Context
	cfg       config
	d         *nativeConn
	committed bool
}

// Ensure session implements the Octobe Session interface.
var _ octobe.Session[Builder] = &nativeSession{}

// Commit commits a transaction. This is a no-op for ClickHouse as it does not support transactions in the same way as
// other databases.
func (s *nativeSession) Commit() error {
	return nil
}

// Rollback rolls back a transaction, this is a no-op for clickhouse as it does not support transactions in the same way
// as other databases.
func (s *nativeSession) Rollback() error {
	return nil
}

// Builder returns a new builder for building queries.
func (s *nativeSession) Builder() Builder {
	return func(query string) Segment {
		return &nativeSegment{
			query: query,
			args:  nil,
			used:  false,
			d:     s.d,
			ctx:   s.ctx,
		}
	}
}

// nativeSegment represents a specific query that can be run only once. It keeps track of the query, arguments, and execution state.
type nativeSegment struct {
	query string
	args  []any
	used  bool
	d     *nativeConn
	ctx   context.Context
}

var _ Segment = &nativeSegment{}

// use sets the Segment as used after it has been performed.
func (s *nativeSegment) use() {
	s.used = true
}

// Arguments sets the arguments to be used in the query.
func (s *nativeSegment) Arguments(args ...any) Segment {
	s.args = args
	return s
}

// Contributors returns the list of contributors for the driver.
func (s *nativeSegment) Contributors() []string {
	return s.d.conn.Contributors()
}

// ServerVersion returns the underlying database server version.
func (s *nativeSegment) ServerVersion() (*ServerVersion, error) {
	return s.d.conn.ServerVersion()
}

// Select executes a query and scans the results into the destination.
func (s *nativeSegment) Select(dest any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	return s.d.conn.Select(s.ctx, dest, s.query, s.args...)
}

// Exec executes a query, typically used for inserts or updates.
func (s *nativeSegment) Exec() error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	return s.d.conn.Exec(s.ctx, s.query, s.args...)
}

// Query performs a normal query against the database that returns rows.
func (s *nativeSegment) Query(cb func(Rows) error) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	var rows driver.Rows
	var err error

	rows, err = s.d.conn.Query(s.ctx, s.query, s.args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err = cb(rows); err != nil {
		return err
	}

	return rows.Err()
}

// QueryRow returns one result and puts it into destination pointers.
func (s *nativeSegment) QueryRow(dest ...any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	row := s.d.conn.QueryRow(s.ctx, s.query, s.args...)
	return row.Scan(dest...)
}

// PrepareBatch prepares a batch for execution. This allows for multiple queries to be executed in a single batch.
func (s *nativeSegment) PrepareBatch(opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if s.used {
		return nil, octobe.ErrAlreadyUsed
	}
	defer s.use()

	batch, err := s.d.conn.PrepareBatch(s.ctx, s.query, opts...)
	if err != nil {
		return nil, err
	}

	return batch, nil
}

// AsyncInsert performs an asynchronous insert operation. If `wait` is true, it will wait for the insert to complete.
func (s *nativeSegment) AsyncInsert(wait bool, args ...any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	if len(args) > 0 {
		s.args = args
	}

	return s.d.conn.AsyncInsert(s.ctx, s.query, wait, s.args...)
}
