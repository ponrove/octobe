package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ponrove/octobe"
)

// Driver represents the Octobe driver for the pgx connection pool.
type Driver octobe.Driver[conn, config, Builder]

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

// Ensure postgres.PGXConn implements the Conn interface.
var _ PGXConn = &pgx.Conn{}

// conn holds the connection and default configuration for the pgx driver.
type conn struct {
	conn PGXConn
}

// Ensure conn implements the Octobe Driver interface.
var _ octobe.Driver[conn, config, Builder] = &conn{}

// OpenPGX creates a new database connection and returns a driver with the specified types.
// It takes a context and a data source name (DSN) as parameters.
// The returned function, when called, initializes a new connection using the provided DSN.
// If the connection creation fails, it returns an error.
// Otherwise, it returns a new conn instance with the created connection.
func OpenPGX(ctx context.Context, dsn string) octobe.Open[conn, config, Builder] {
	return func() (octobe.Driver[conn, config, Builder], error) {
		pgxConn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}

		return &conn{
			conn: pgxConn,
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
func OpenWithOptions(ctx context.Context, dsn string, options ParseConfigOptions) octobe.Open[conn, config, Builder] {
	return func() (octobe.Driver[conn, config, Builder], error) {
		pgxConn, err := pgx.ConnectWithOptions(ctx, dsn, pgx.ParseConfigOptions{ParseConfigOptions: options.ParseConfigOptions})
		if err != nil {
			return nil, err
		}

		return &conn{
			conn: pgxConn,
		}, nil
	}
}

// OpenPGXWithConn creates a new database connection using an existing connection.
// It takes an existing connection as a parameter.
// The returned function, when called, returns a new conn instance with the provided connection.
// If the provided connection is nil, it returns an error.
func OpenPGXWithConn(c PGXConn) octobe.Open[conn, config, Builder] {
	return func() (octobe.Driver[conn, config, Builder], error) {
		if c == nil {
			return nil, errors.New("conn is nil")
		}

		return &conn{
			conn: c,
		}, nil
	}
}

// config defines various configurations possible for the pgx driver.
type config struct {
	txOptions *TxOptions
}

// TxOptions holds the options for a transaction.
type TxOptions pgx.TxOptions

// WithTransaction enables the use of a transaction for the session, enforcing the usage of commit and rollback.
func WithTransaction(options TxOptions) octobe.Option[config] {
	return func(c *config) {
		c.txOptions = &options
	}
}

// Begin starts a new session with the database and returns a Session instance.
// It takes a context and optional configuration options as parameters.
// If transaction options are provided, it begins a transaction with those options, otherwise it starts a
// non-transactional session. If the transaction initiation fails, it returns an error.
func (d *conn) Begin(ctx context.Context, opts ...octobe.Option[config]) (octobe.Session[Builder], error) {
	var cfg config
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

	return &session{
		ctx: ctx,
		cfg: cfg,
		tx:  tx,
		d:   d,
	}, nil
}

// Close closes the database connection.
func (d *conn) Close(ctx context.Context) error {
	return d.conn.Close(ctx)
}

// session holds session context, representing a series of related queries.
// A session can be transactional or non-transactional. If transactional, it enforces the usage of commit and rollback.
// A session is not thread-safe and should only be used in one thread at a time.
type session struct {
	ctx       context.Context
	cfg       config
	tx        pgx.Tx
	d         *conn
	committed bool
}

// Ensure session implements the Octobe Session interface.
var _ octobe.Session[Builder] = &session{}

// Commit commits a transaction. This only works if the session is transactional.
func (s *session) Commit() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot commit without transaction")
	}
	defer func() {
		s.committed = true
	}()
	return s.tx.Commit(s.ctx)
}

// Rollback rolls back a transaction. This only works if the session is transactional.
func (s *session) Rollback() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot rollback without transaction")
	}
	return s.tx.Rollback(s.ctx)
}

// Builder is a function signature used for building queries with the pgx driver.
type Builder func(query string) Segment

// Builder returns a new builder for building queries.
func (s *session) Builder() Builder {
	return func(query string) Segment {
		return Segment{
			query: query,
			args:  nil,
			used:  false,
			tx:    s.tx,
			d:     s.d,
			ctx:   s.ctx,
		}
	}
}

// Handler is a signature type for a handler. The handler receives a builder of the specific driver and returns a result and an error.
type Handler[RESULT any] func(Builder) (RESULT, error)

// Execute executes a handler with a session builder, injecting the builder of the driver into the handler.
func Execute[RESULT any](session octobe.BuilderSession[Builder], f Handler[RESULT]) (RESULT, error) {
	return f(session.Builder())
}

// Segment represents a specific query that can be run only once. It keeps track of the query, arguments, and execution state.
type Segment struct {
	query string          // SQL query to be executed
	args  []any           // Argument values
	used  bool            // Indicates if this Segment has been executed
	tx    pgx.Tx          // Database transaction, initiated by BeginTx
	d     *conn           // Driver used for the session
	ctx   context.Context // Context to interrupt a query
}

// use sets the Segment as used after it has been performed.
func (s *Segment) use() {
	s.used = true
}

// Arguments sets the arguments to be used in the query.
func (s *Segment) Arguments(args ...any) *Segment {
	s.args = args
	return s
}

// Exec executes a query, typically used for inserts or updates.
func (s *Segment) Exec() (pgconn.CommandTag, error) {
	if s.used {
		return pgconn.CommandTag{}, octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		return s.d.conn.Exec(s.ctx, s.query, s.args...)
	}
	return s.tx.Exec(s.ctx, s.query, s.args...)
}

// QueryRow returns one result and puts it into destination pointers.
func (s *Segment) QueryRow(dest ...any) error {
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
func (s *Segment) Query(cb func(pgx.Rows) error) error {
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
