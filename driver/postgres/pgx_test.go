package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
	"github.com/ponrove/octobe/driver/postgres/mock"
	"github.com/stretchr/testify/assert"
)

func TestPGXWithTxInsideStartTransaction(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	name := "Some name"

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectQueryRow("SELECT id, name FROM products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXWithTx(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	name := "Some name"

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectQueryRow("SELECT id, name FROM products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestPGXWithoutTx(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	name := "Some name"

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectQueryRow("SELECT id, name FROM products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestPGXWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(pgxmock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestPGXWithTxManualRollback(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	name := "Some name"

	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewMockRow(1, name))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestPGXWithoutTxCommit(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestPGXWithoutTxRollback(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestSegmentUsedTwice(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		m.ExpectExec("CREATE TABLE").WillReturnResult(mock.NewResult("", 0))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		name := "Some name"

		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewMockRow(1, name))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		m.ExpectQuery("SELECT").WillReturnRows(mock.NewMockRows([]string{"id", "name"}))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestOpenWithConnNil(t *testing.T) {
	_, err := octobe.New(postgres.OpenPGXWithConn(nil))
	assert.Error(t, err)
	assert.Equal(t, "conn is nil", err.Error())
}

func TestBeginError(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	expectedErr := errors.New("begin error")
	m.ExpectBeginTx(postgres.PGXTxOptions{}).WillReturnError(expectedErr)
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestCommitError(t *testing.T) {
	m := mock.NewPGXMock()
	ctx := context.Background()
	defer m.Close(ctx)

	expectedErr := errors.New("commit error")
	m.ExpectBeginTx(postgres.PGXTxOptions{})
	m.ExpectCommit().WillReturnError(expectedErr)
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestSegmentExecError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("exec error")
		m.ExpectExec("INSERT").WillReturnError(expectedErr)
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("exec error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectExec("INSERT").WillReturnError(expectedErr)
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestSegmentQueryRowError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("query row error")
		m.ExpectQueryRow("SELECT").WillReturnError(expectedErr)
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("query row error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectQueryRow("SELECT").WillReturnError(expectedErr)
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

func TestSegmentQueryError(t *testing.T) {
	t.Run("query error without tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("query error")
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("query error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("callback error")
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewMockRows([]string{"id"}).AddRow(1))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
		m := mock.NewPGXMock()
		ctx := context.Background()
		defer m.Close(ctx)

		expectedErr := errors.New("callback error")
		m.ExpectBeginTx(postgres.PGXTxOptions{})
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewMockRows([]string{"id"}).AddRow(1))
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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
