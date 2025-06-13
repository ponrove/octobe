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

func TestPGXPoolWithTxInsideStartTransaction(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	name := "Some name"

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectCommit()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

	assert.NoError(t, err)

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPGXPoolWithTx(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	name := "Some name"

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectCommit()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolWithoutTx(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	defer mock.Close()

	name := "Some name"

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectRollback()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectRollback()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolWithTxManualRollback(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	name := "Some name"

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectQuery("INSERT INTO products").WithArgs(name).WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))
	mock.ExpectRollback()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolWithoutTxCommit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolWithoutTxRollback(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolSegmentUsedTwice(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		mock.ExpectExec("CREATE TABLE").WillReturnResult(pgxmock.NewResult("", 0))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		name := "Some name"

		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, name))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id", "name"}))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestOpenPGXPoolWithPoolNil(t *testing.T) {
	_, err := octobe.New(postgres.OpenPGXPoolWithPool(nil))
	assert.Error(t, err)
	assert.Equal(t, "pool is nil", err.Error())
}

func TestPGXPoolBeginError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	expectedErr := errors.New("begin error")
	mock.ExpectBeginTx(pgx.TxOptions{}).WillReturnError(expectedErr)

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPGXPoolCommitError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	ctx := context.Background()
	defer mock.Close()

	expectedErr := errors.New("commit error")
	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectCommit().WillReturnError(expectedErr)

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolSegmentExecError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("exec error")
		mock.ExpectExec("INSERT").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("exec error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectExec("INSERT").WillReturnError(expectedErr)
		mock.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolSegmentQueryRowError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("query row error")
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("query row error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)
		mock.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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

func TestPGXPoolSegmentQueryError(t *testing.T) {
	t.Run("query error without tx", func(t *testing.T) {
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("query error")
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("query error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectQuery("SELECT").WillReturnError(expectedErr)
		mock.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("callback error")
		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
		mock, err := pgxmock.NewPool()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		ctx := context.Background()
		defer mock.Close()

		expectedErr := errors.New("callback error")
		mock.ExpectBeginTx(pgx.TxOptions{})
		mock.ExpectQuery("SELECT").WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
		mock.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(mock))
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
