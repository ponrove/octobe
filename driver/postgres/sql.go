package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/ponrove/octobe"
)

var _ SQL = (*sql.DB)(nil)

// SQL defines the interface for the database/sql connection.
type SQL interface {
	driver.ConnBeginTx
	driver.ExecerContext
	driver.QueryerContext
	driver.Conn
	driver.ConnPrepareContext
	PingContext(ctx context.Context) error
	/*
		Begin() (driver.Tx, error)
		BeginTx(context.Context, driver.TxOptions) (driver.Tx, error)
		Close() error
		PingContext(ctx context.Context) error
		SetConnMaxLifetime(d time.Duration)
		SetMaxIdleConns(n int)
		SetMaxOpenConns(n int)
		Stats() sql.DBStats
		Exec(query string, args ...any) (driver.Result, error)
		ExecContext(ctx context.Context, query string, args ...any) (driver.Result, error)
		Prepare(query string) (driver.Stmt, error)
		PrepareContext(ctx context.Context, query string) (driver.Stmt, error)
		Query(query string, args ...any) (driver.Rows, error)
		QueryContext(ctx context.Context, query string, args ...any) (driver.Rows, error)
		QueryRow(query string, args ...any) driver.Row
		QueryRowContext(ctx context.Context, query string, args ...any) driver.Row
	*/
}

// sqlConn holds the connection db and default configuration for the sqlConn driver
type sqlConn struct {
	sqlDB SQL
}

// Type check to make sure that the conn driver implements the Octobe Driver interface
var _ SQLDriver = &sqlConn{}

// OpenWithConn is a function that can be used for opening a new database connection, it should always return a driver
// with set signature of types for the local driver. This function is used when a connection db is already available.
func OpenWithConn(db SQL) octobe.Open[sqlConn, sqlConfig, Builder] {
	return func() (octobe.Driver[sqlConn, sqlConfig, Builder], error) {
		if db == nil {
			return nil, errors.New("db is nil")
		}

		return &sqlConn{
			sqlDB: db,
		}, nil
	}
}

// Begin will start a new session with the database, this will return a Session instance that can be used for handling
// queries. Options can be passed to the driver for specific configuration that overwrites the default configuration
// given at instantiation of the Octobe instance. If no options are passed, the default configuration will be used.
// If the default configuration is not set, the session will not be transactional.
func (d *sqlConn) Begin(ctx context.Context, opts ...octobe.Option[sqlConfig]) (octobe.Session[Builder], error) {
	var cfg sqlConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var tx driver.Tx
	var err error
	if cfg.txOptions != nil {
		tx, err = d.sqlDB.BeginTx(ctx, driver.TxOptions{
			Isolation: cfg.txOptions.Isolation,
			ReadOnly:  cfg.txOptions.ReadOnly,
		})
	}

	if err != nil {
		return nil, err
	}

	return &sqlSession{
		ctx: ctx,
		cfg: cfg,
		tx:  tx,
		d:   d,
	}, nil
}

// Close will close the database connection.
func (d *sqlConn) Close(_ context.Context) error {
	return d.sqlDB.Close()
}

// Ping will ping the database connection to check if it is alive.
func (d *sqlConn) Ping(ctx context.Context) error {
	if d.sqlDB == nil {
		return errors.New("connection is nil")
	}
	return d.sqlDB.PingContext(ctx)
}

// sqlSession is a struct that holds sqlSession context, a sqlSession should be considered a series of queries that are related
// to each other. A sqlSession can be transactional or non-transactional, if it is transactional, it will enforce the usage
// of commit and rollback. If it is non-transactional, it will not enforce the usage of commit and rollback.
// A sqlSession is not thread safe, it should only be used in one thread at a time.
type sqlSession struct {
	ctx       context.Context
	cfg       sqlConfig
	tx        driver.Tx
	d         *sqlConn
	committed bool
}

// Type check to make sure that the session implements the Octobe Session interface
var _ octobe.Session[Builder] = &sqlSession{}

// Commit will commit a transaction, this will only work if the session is transactional.
func (s *sqlSession) Commit() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot commit without transaction")
	}
	defer func() {
		s.committed = true
	}()
	return s.tx.Commit()
}

// Rollback will rollback a transaction, this will only work if the session is transactional.
func (s *sqlSession) Rollback() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot rollback without transaction")
	}
	return s.tx.Rollback()
}

// Builder will return a new builder for building queries
func (s *sqlSession) Builder() Builder {
	return func(query string) Segment {
		return &sqlSegment{
			query: query,
			args:  nil,
			used:  false,
			tx:    s.tx,
			d:     s.d,
			ctx:   s.ctx,
		}
	}
}

type txContext interface {
	driver.Tx
	driver.ExecerContext
	driver.QueryerContext
}

// Segment is a specific query that can be run only once it keeps a few fields for keeping track on the Segment
type sqlSegment struct {
	// query in SQL that is going to be executed
	query string
	// args include argument values
	args []driver.NamedValue
	// used specify if this Segment already has been executed
	used bool
	// tx is the database transaction, initiated by BeginTx
	tx txContext
	// d is the driver that is used for the session
	d *sqlConn
	// ctx is a context that can be used to interrupt a query
	ctx context.Context
}

var _ Segment = &pgxSegment{}

// use will set used to true after a Segment has been performed
func (s *sqlSegment) use() {
	s.used = true
}

// Arguments receives unknown amount of arguments to use in the query
func (s *sqlSegment) Arguments(args ...driver.NamedValue) Segment {
	s.args = args
	return s
}

// Exec will execute a query. Used for inserts or updates
func (s *sqlSegment) Exec() (ExecResult, error) {
	if s.used {
		return ExecResult{}, octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		res, err := s.d.sqlDB.ExecContext(s.ctx, s.query, s.args)
		if err != nil {
			return ExecResult{}, err
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return ExecResult{}, fmt.Errorf("failed to get rows affected: %w", err)
		}

		return ExecResult{
			RowsAffected: rowsAffected,
		}, nil
	}

	// If we have a transaction, we execute the query in the transaction context
	res, err := s.tx.ExecContext(s.ctx, s.query, s.args)
	if err != nil {
		return ExecResult{}, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return ExecResult{}, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return ExecResult{
		RowsAffected: rowsAffected,
	}, nil
}

// QueryRow will return one result and put them into destination pointers
func (s *sqlSegment) QueryRow(dest ...any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		return s.d.sqlDB.QueryRowContext(s.ctx, s.query, s.args...).Scan(dest...)
	}
	return s.tx.QueryRowContext(s.ctx, s.query, s.args...).Scan(dest...)
}

// Query will perform a normal query against database that returns rows
func (s *sqlSegment) Query(cb func(Rows) error) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	var err error
	var rows *driver.Rows
	if s.tx == nil {
		rows, err = s.d.sqlDB.QueryContext(s.ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	} else {
		rows, err = s.tx.QueryContext(s.ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	}

	if err = cb(rows); err != nil {
		err2 := rows.Close()
		return fmt.Errorf("error in callback: %w, error in closing rows: %w", err, err2)
	}

	return rows.Close()
}
