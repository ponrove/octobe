package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
	"github.com/ponrove/octobe/driver/postgres/mock"
	"github.com/stretchr/testify/assert"
)

func TestPGXPoolWithTxInsideStartTransaction(t *testing.T) {
	m := mock.NewPGXPoolMock()
	defer m.Close()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx(postgres.PGXTxOptions{
		IsoLevel: postgres.ReadCommitted,
	})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgconn.NewCommandTag(""))
	m.ExpectQueryRow("INSERT INTO products (name) VALUES ($1) RETURNING id, name;").WithArgs(name).WillReturnRow(mock.NewMockRow(int(1), name))
	m.ExpectQueryRow("SELECT id, name FROM products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectCommit()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{
		IsoLevel: postgres.ReadCommitted,
	}))

	assert.NoError(t, err)

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithTx(t *testing.T) {
	m := mock.NewPGXPoolMock()
	defer m.Close()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products (name) VALUES ($1) RETURNING id, name;").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectQueryRow("SELECT id, name FROM products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectCommit()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithoutTx(t *testing.T) {
	m := mock.NewPGXPoolMock()
	defer m.Close()

	ctx := context.Background()

	name := "Some name"

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products (name) VALUES ($1) RETURNING id, name;").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectQueryRow("SELECT id, name FROM products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()
	defer m.Close()

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()
	defer m.Close()

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	panicMsg := "oh no!"
	defer func() {
		p := recover()
		assert.Equal(t, panicMsg, p)

		err = ob.Close(ctx)
		assert.NoError(t, err)
		assert.NoError(t, m.AllExpectationsMet())
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
	m := mock.NewPGXPoolMock()
	ctx := context.Background()
	defer m.Close()

	name := "Some name"

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products (name) VALUES ($1) RETURNING id, name;").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectRollback()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithoutTxCommit(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()
	defer m.Close()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithoutTxRollback(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()
	defer m.Close()

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolSegmentUsedTwice(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		m.ExpectExec("CREATE TABLE").WillReturnResult(mock.NewResult("", 0))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("QueryRow", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		name := "Some name"

		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewMockRow(1, name))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("Query", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		m.ExpectQuery("SELECT").WillReturnRows(mock.NewMockRows([]string{"id", "name"}))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestOpenPGXPoolWithPoolNil(t *testing.T) {
	_, err := octobe.New(postgres.OpenPGXPoolWithPool(nil))
	assert.Error(t, err)
	assert.Equal(t, "pool is nil", err.Error())
}

func TestPGXPoolBeginError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()
	defer m.Close()

	expectedErr := errors.New("begin error")
	m.ExpectBeginTx(postgres.PGXTxOptions{}).WillReturnError(expectedErr)

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolCommitError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()
	defer m.Close()

	expectedErr := errors.New("commit error")
	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectCommit().WillReturnError(expectedErr)

	ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolSegmentExecError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("exec error")
		m.ExpectExec("INSERT").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("with tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("exec error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectExec("INSERT").WillReturnError(expectedErr)
		m.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestPGXPoolSegmentQueryRowError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("query row error")
		m.ExpectQueryRow("SELECT").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("with tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("query row error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectQueryRow("SELECT").WillReturnError(expectedErr)
		m.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestPGXPoolSegmentQueryError(t *testing.T) {
	t.Run("query error without tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("query error")
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("query error with tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("query error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)
		m.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("callback error without tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("callback error")
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewMockRows([]string{"id"}).AddRow(1))

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("callback error with tx", func(t *testing.T) {
		m := mock.NewPGXPoolMock()
		ctx := context.Background()
		defer m.Close()

		expectedErr := errors.New("callback error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewMockRows([]string{"id"}).AddRow(1))
		m.ExpectRollback()

		ob, err := octobe.New(postgres.OpenPGXPoolWithPool(m))
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

		assert.NoError(t, m.AllExpectationsMet())
	})
}
