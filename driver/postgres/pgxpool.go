package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ponrove/octobe"
)

// PGXPool defines the interface for a connection pool.
type PGXPool interface {
	Close()
	Acquire(ctx context.Context) (c *pgxpool.Conn, err error)
	AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error
	AcquireAllIdle(ctx context.Context) []*pgxpool.Conn
	Reset()
	Config() *pgxpool.Config
	Stat() *pgxpool.Stat
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Ping(ctx context.Context) error
}

var _ PGXPool = &pgxpool.Pool{} // Ensure pgxpool.Pool implements the PGXPool interface.

// conn holds the connection pool and default configuration for the conn driver.
type pgxpoolConn struct {
	pool PGXPool
}

// Ensure conn implements the octobe.Driver interface.
var _ octobe.Driver[pgxpoolConn, pgxConfig, Builder] = &pgxpoolConn{}

// Open creates a new database connection and returns a driver with the specified types.
func OpenPGXPool(ctx context.Context, dsn string) octobe.Open[pgxpoolConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxpoolConn, pgxConfig, Builder], error) {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, err
		}

		return &pgxpoolConn{
			pool: pool,
		}, nil
	}
}

// OpenWithPool creates a new database connection using an existing connection pool.
func OpenPGXPoolWithPool(pool PGXPool) octobe.Open[pgxpoolConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxpoolConn, pgxConfig, Builder], error) {
		if pool == nil {
			return nil, errors.New("pool is nil")
		}

		return &pgxpoolConn{
			pool: pool,
		}, nil
	}
}

// Begin starts a new session with the database and returns a Session instance.
func (d *pgxpoolConn) Begin(ctx context.Context, opts ...octobe.Option[pgxConfig]) (octobe.Session[Builder], error) {
	var cfg pgxConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var tx pgx.Tx
	var err error
	if cfg.txOptions != nil {
		tx, err = d.pool.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:       cfg.txOptions.IsoLevel,
			AccessMode:     cfg.txOptions.AccessMode,
			DeferrableMode: cfg.txOptions.DeferrableMode,
			BeginQuery:     cfg.txOptions.BeginQuery,
		})
	}

	if err != nil {
		return nil, err
	}

	return &pgxpoolSession{
		ctx: ctx,
		cfg: cfg,
		tx:  tx,
		d:   d,
	}, nil
}

// Close closes the database connection.
func (d *pgxpoolConn) Close(_ context.Context) error {
	d.pool.Close()
	return nil
}

// Ping checks the connection to the database.
func (d *pgxpoolConn) Ping(ctx context.Context) error {
	if d.pool == nil {
		return errors.New("pool is nil")
	}
	return d.pool.Ping(ctx)
}

// session holds session context and manages a series of related queries.
type pgxpoolSession struct {
	ctx       context.Context
	cfg       pgxConfig
	tx        pgx.Tx
	d         *pgxpoolConn
	committed bool
}

// Ensure session implements the octobe.Session interface.
var _ octobe.Session[Builder] = &pgxpoolSession{}

// Commit commits a transaction if the session is transactional.
func (s *pgxpoolSession) Commit() error {
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

// Rollback rolls back a transaction if the session is transactional.
func (s *pgxpoolSession) Rollback() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot rollback without transaction")
	}
	return s.tx.Rollback(s.ctx)
}

// Builder returns a new builder for building queries.
func (s *pgxpoolSession) Builder() Builder {
	return func(query string) Segment {
		return &pgxpoolSegment{
			query: query,
			args:  nil,
			used:  false,
			tx:    s.tx,
			d:     s.d,
			ctx:   s.ctx,
		}
	}
}

// Segment represents a specific query that can be run only once.
type pgxpoolSegment struct {
	query string          // SQL query to be executed
	args  []any           // Argument values for the query
	used  bool            // Indicates if the Segment has been executed
	tx    pgx.Tx          // Database transaction, initiated by BeginTx
	d     *pgxpoolConn    // Driver used for the session
	ctx   context.Context // Context to interrupt a query
}

var _ Segment = &pgxpoolSegment{}

// use sets used to true after a Segment has been performed.
func (s *pgxpoolSegment) use() {
	s.used = true
}

// Arguments sets the arguments for the query.
func (s *pgxpoolSegment) Arguments(args ...any) Segment {
	s.args = args
	return s
}

// Exec executes a query for inserts or updates.
func (s *pgxpoolSegment) Exec() (ExecResult, error) {
	if s.used {
		return ExecResult{}, octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		res, err := s.d.pool.Exec(s.ctx, s.query, s.args...)
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
func (s *pgxpoolSegment) QueryRow(dest ...any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		return s.d.pool.QueryRow(s.ctx, s.query, s.args...).Scan(dest...)
	}
	return s.tx.QueryRow(s.ctx, s.query, s.args...).Scan(dest...)
}

// Query performs a normal query against the database that returns rows.
func (s *pgxpoolSegment) Query(cb func(Rows) error) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	var err error
	var rows pgx.Rows
	if s.tx == nil {
		rows, err = s.d.pool.Query(s.ctx, s.query, s.args...)
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
