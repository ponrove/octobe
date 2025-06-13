package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
	"github.com/stretchr/testify/assert"
)

func TestPGXWithTxInsideStartTransaction(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)

	name := "Some name"

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectCommit()
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err = postgres.Execute(session, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		product1, err := postgres.Execute(session, AddProduct(name))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		assert.Equal(t, name, product1.Name)
		assert.NotZero(t, product1.ID)

		product2, err := postgres.Execute(session, ProductByName(name))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		assert.Equal(t, name, product2.Name)
		assert.NotZero(t, product2.ID)
		return nil
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPGXWithTx(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)

	name := "Some name"

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectCommit()
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = postgres.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	product1, err := postgres.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product1.Name)
	assert.NotZero(t, product1.ID)

	product2, err := postgres.Execute(session, ProductByName(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product2.Name)
	assert.NotZero(t, product2.ID)

	err = session.Commit()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPGXWithoutTx(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	defer mock.Close(ctx)

	name := "Some name"

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = postgres.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	product1, err := postgres.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product1.Name)
	assert.NotZero(t, product1.ID)

	product2, err := postgres.Execute(session, ProductByName(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product2.Name)
	assert.NotZero(t, product2.ID)

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func Migration() postgres.Handler[octobe.Void] {
	return func(builder postgres.Builder) (octobe.Void, error) {
		query := builder(`
			CREATE TABLE IF NOT EXISTS products (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		_, err := query.Exec()
		return nil, err
	}
}

type Product struct {
	ID   int
	Name string
}

func AddProduct(name string) postgres.Handler[Product] {
	return func(builder postgres.Builder) (Product, error) {
		var product Product
		query := builder(`
			INSERT INTO products (name) VALUES ($1) RETURNING id, name;
		`)

		query.Arguments(name)
		err := query.QueryRow(&product.ID, &product.Name)
		return product, err
	}
}

func ProductByName(name string) postgres.Handler[Product] {
	return func(builder postgres.Builder) (Product, error) {
		var product Product
		query := builder(`
			SELECT id, name FROM products WHERE name = $1;
		`)

		query.Arguments(name)
		err := query.QueryRow(&product.ID, &product.Name)
		return product, err
	}
}

func TestPGXWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectRollback()
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	expectedErr := errors.New("something went wrong")
	err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err = postgres.Execute(session, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		return expectedErr
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

	assert.Equal(t, expectedErr, err)

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPGXWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectRollback()
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	panicMsg := "oh no!"
	defer func() {
		p := recover()
		assert.Equal(t, panicMsg, p)

		err = ob.Close(ctx)
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	}()

	_ = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err = postgres.Execute(session, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		panic(panicMsg)
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
}

func TestPGXWithTxManualRollback(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)

	name := "Some name"

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectRollback()
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = postgres.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = postgres.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Rollback()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPGXWithoutTxCommit(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit()
	assert.Error(t, err)
	assert.Equal(t, "cannot commit without transaction", err.Error())

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPGXWithoutTxRollback(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Rollback()
	assert.Error(t, err)
	assert.Equal(t, "cannot rollback without transaction", err.Error())

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSegmentUsedTwice(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		mock.ExpectExec("CREATE TABLE").WillReturnResult(pgxmock.NewResult("", 0))
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(builder postgres.Builder) (octobe.Void, error) {
			query := builder(`CREATE TABLE`)
			_, err := query.Exec()
			if err != nil {
				return nil, err
			}
			// Use it again
			_, err = query.Exec()
			return nil, err
		}

		_, err = postgres.Execute(session, handler)
		assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("QueryRow", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		name := "Some name"

		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(builder postgres.Builder) (octobe.Void, error) {
			query := builder(`SELECT`)
			var p Product
			err := query.QueryRow(&p.ID, &p.Name)
			if err != nil {
				return nil, err
			}
			// Use it again
			err = query.QueryRow(&p.ID, &p.Name)
			return nil, err
		}

		_, err = postgres.Execute(session, handler)
		assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Query", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id", "name"}))
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(builder postgres.Builder) (octobe.Void, error) {
			query := builder(`SELECT`)
			err := query.Query(func(rows postgres.Rows) error {
				return nil
			})
			if err != nil {
				return nil, err
			}
			// Use it again
			err = query.Query(func(rows postgres.Rows) error {
				return nil
			})
			return nil, err
		}

		_, err = postgres.Execute(session, handler)
		assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestOpenWithConnNil(t *testing.T) {
	_, err := octobe.New(postgres.OpenPGXWithConn(nil))
	assert.Error(t, err)
	assert.Equal(t, "conn is nil", err.Error())
}

func TestBeginError(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)

	expectedErr := errors.New("begin error")
	mock.ExpectBeginTx(pgx.TxOptions{}).WillReturnError(expectedErr)
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCommitError(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close(ctx)

	expectedErr := errors.New("commit error")
	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectCommit().WillReturnError(expectedErr)
	mock.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit()
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSegmentExecError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("exec error")
		mock.ExpectExec("INSERT").WillReturnError(expectedErr)
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = postgres.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
			query := builder("INSERT")
			_, err := query.Exec()
			return nil, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("with tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("exec error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectExec("INSERT").WillReturnError(expectedErr)
		mock.ExpectRollback()
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := postgres.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
				query := builder("INSERT")
				_, err := query.Exec()
				return nil, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestSegmentQueryRowError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("query row error")
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = postgres.Execute(session, func(builder postgres.Builder) (Product, error) {
			var p Product
			query := builder("SELECT")
			err := query.QueryRow(&p.ID, &p.Name)
			return p, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("with tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("query row error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)
		mock.ExpectRollback()
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := postgres.Execute(session, func(builder postgres.Builder) (Product, error) {
				var p Product
				query := builder("SELECT")
				err := query.QueryRow(&p.ID, &p.Name)
				return p, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestSegmentQueryError(t *testing.T) {
	t.Run("query error without tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("query error")
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = postgres.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
			query := builder("SELECT")
			err := query.Query(func(rows postgres.Rows) error { return nil })
			return nil, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("query error with tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("query error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)
		mock.ExpectRollback()
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := postgres.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
				query := builder("SELECT")
				err := query.Query(func(rows postgres.Rows) error { return nil })
				return nil, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("callback error without tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("callback error")
		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = postgres.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
			query := builder("SELECT")
			err := query.Query(func(rows postgres.Rows) error { return expectedErr })
			return nil, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("callback error with tx", func(t *testing.T) {
		mock, err := pgxmock.NewConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close(ctx)

		expectedErr := errors.New("callback error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
		mock.ExpectRollback()
		mock.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(mock))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := postgres.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
				query := builder("SELECT")
				err := query.Query(func(rows postgres.Rows) error { return expectedErr })
				return nil, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
