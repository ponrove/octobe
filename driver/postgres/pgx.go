package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ponrove/octobe"
)

// PGXConn defines the interface for a PGX postgres connection.
type PGXConn interface {
	Close(context.Context) error
	Prepare(context.Context, string, string) (*pgconn.StatementDescription, error)
	Deallocate(context.Context, string) error
	DeallocateAll(context.Context) error
	Ping(context.Context) error
	PgConn() *pgconn.PgConn
	Config() *pgx.ConnConfig
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

var _ PGXConn = &pgx.Conn{} // Ensure pgx.Conn implements the PGXConn interface.

// conn holds the connection and default configuration for the pgx driver.
type pgxConn struct {
	conn PGXConn
}

// Ensure conn implements the Octobe Driver interface.
var _ octobe.Driver[pgxConn, pgxConfig, Builder] = &pgxConn{}

// OpenPGX creates a new database connection and returns a driver with the specified types.
// It takes a context and a data source name (DSN) as parameters.
// The returned function, when called, initializes a new connection using the provided DSN.
// If the connection creation fails, it returns an error.
// Otherwise, it returns a new conn instance with the created connection.
func OpenPGX(ctx context.Context, dsn string) octobe.Open[pgxConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxConn, pgxConfig, Builder], error) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}

		return &pgxConn{
			conn: conn,
		}, nil
	}
}

// ParseConfigOptions contains options that control how a config is built such as getsslpassword.
type ParseConfigOptions struct {
	pgconn.ParseConfigOptions
}

// OpenWithOptions creates a new database connection with additional options and returns a driver with the specified types.
// It takes a context, a data source name (DSN), and additional parse config options as parameters.
// The returned function, when called, initializes a new connection using the provided DSN and options.
// If the connection creation fails, it returns an error.
// Otherwise, it returns a new conn instance with the created connection.
func OpenPGXWithOptions(ctx context.Context, dsn string, options ParseConfigOptions) octobe.Open[pgxConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxConn, pgxConfig, Builder], error) {
		conn, err := pgx.ConnectWithOptions(ctx, dsn, pgx.ParseConfigOptions{ParseConfigOptions: options.ParseConfigOptions})
		if err != nil {
			return nil, err
		}

		return &pgxConn{
			conn: conn,
		}, nil
	}
}

// OpenPGXWithConn creates a new database connection using an existing connection.
// It takes an existing connection as a parameter.
// The returned function, when called, returns a new conn instance with the provided connection.
// If the provided connection is nil, it returns an error.
func OpenPGXWithConn(c PGXConn) octobe.Open[pgxConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxConn, pgxConfig, Builder], error) {
		if c == nil {
			return nil, errors.New("conn is nil")
		}

		return &pgxConn{
			conn: c,
		}, nil
	}
}

// Begin starts a new session with the database and returns a Session instance.
// It takes a context and optional configuration options as parameters.
// If transaction options are provided, it begins a transaction with those options, otherwise it starts a
// non-transactional session. If the transaction initiation fails, it returns an error.
func (d *pgxConn) Begin(ctx context.Context, opts ...octobe.Option[pgxConfig]) (octobe.Session[Builder], error) {
	var cfg pgxConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var tx pgx.Tx
	var err error
	if cfg.txOptions != nil {
		tx, err = d.conn.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:       cfg.txOptions.IsoLevel,
			AccessMode:     cfg.txOptions.AccessMode,
			DeferrableMode: cfg.txOptions.DeferrableMode,
			BeginQuery:     cfg.txOptions.BeginQuery,
		})
	}

	if err != nil {
		return nil, err
	}

	return &pgxSession{
		ctx: ctx,
		cfg: cfg,
		tx:  tx,
		d:   d,
	}, nil
}

// Close closes the database connection.
func (d *pgxConn) Close(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Close(ctx)
}

func (d *pgxConn) Ping(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Ping(ctx)
}

// pgxSession holds pgxSession context, representing a series of related queries.
// A pgxSession can be transactional or non-transactional. If transactional, it enforces the usage of commit and rollback.
// A pgxSession is not thread-safe and should only be used in one thread at a time.
type pgxSession struct {
	ctx       context.Context
	cfg       pgxConfig
	tx        pgx.Tx
	d         *pgxConn
	committed bool
}

// Ensure session implements the Octobe Session interface.
var _ octobe.Session[Builder] = &pgxSession{}

// Commit commits a transaction. This only works if the session is transactional.
func (s *pgxSession) Commit() error {
	if s.committed {
		return errors.New("cannot commit a session that has already been committed")
	}

	if s.cfg.txOptions == nil {
		return errors.New("cannot commit without transaction")
	}
	defer func() {
		s.committed = true
	}()
	return s.tx.Commit(s.ctx)
}

// Rollback rolls back a transaction. This only works if the session is transactional.
func (s *pgxSession) Rollback() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot rollback without transaction")
	}
	return s.tx.Rollback(s.ctx)
}

// Builder returns a new builder for building queries.
func (s *pgxSession) Builder() Builder {
	return func(query string) Segment {
		return &pgxSegment{
			query: query,
			args:  nil,
			used:  false,
			tx:    s.tx,
			d:     s.d,
			ctx:   s.ctx,
		}
	}
}

// Segment represents a specific query that can be run only once. It keeps track of the query, arguments, and execution state.
type pgxSegment struct {
	query string          // SQL query to be executed
	args  []any           // Argument values
	used  bool            // Indicates if this Segment has been executed
	tx    pgx.Tx          // Database transaction, initiated by BeginTx
	d     *pgxConn        // Driver used for the session
	ctx   context.Context // Context to interrupt a query
}

var _ Segment = &pgxSegment{}

// use sets the Segment as used after it has been performed.
func (s *pgxSegment) use() {
	s.used = true
}

// Arguments sets the arguments to be used in the query.
func (s *pgxSegment) Arguments(args ...any) Segment {
	s.args = args
	return s
}

// Exec executes a query, typically used for inserts or updates.
func (s *pgxSegment) Exec() (ExecResult, error) {
	if s.used {
		return ExecResult{}, octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		res, err := s.d.conn.Exec(s.ctx, s.query, s.args...)
		if err != nil {
			return ExecResult{}, err
		}

		return ExecResult{
			RowsAffected: res.RowsAffected(),
		}, nil
	}

	res, err := s.tx.Exec(s.ctx, s.query, s.args...)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{
		RowsAffected: res.RowsAffected(),
	}, nil
}

// QueryRow returns one result and puts it into destination pointers.
func (s *pgxSegment) QueryRow(dest ...any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		return s.d.conn.QueryRow(s.ctx, s.query, s.args...).Scan(dest...)
	}
	return s.tx.QueryRow(s.ctx, s.query, s.args...).Scan(dest...)
}

// Query performs a normal query against the database that returns rows.
func (s *pgxSegment) Query(cb func(Rows) error) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	var err error
	var rows pgx.Rows
	if s.tx == nil {
		rows, err = s.d.conn.Query(s.ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	} else {
		rows, err = s.tx.Query(s.ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	}

	defer rows.Close()
	if err = cb(rows); err != nil {
		return err
	}

	return nil
}
