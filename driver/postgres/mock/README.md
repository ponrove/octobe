## Testing

Octobe includes built-in mock clients for drivers to facilitate testing your database logic without needing a live database connection. The API is heavily inspired by the popular [`go-sqlmock`](https://github.com/DATA-DOG/go-sqlmock) library.

### Postgres Mock Example

Here’s how you can test a handler that adds a product using the `postgres` mock for `pgxpool`.

```/dev/null/postgres_test.go
package main

import (
	"context"
	"testing"

	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
	"github.com/ponrove/octobe/driver/postgres/mock"
	"github.com/stretchr/testify/require"
)

// Product is a model that represents a product in the database.
type Product struct {
	ID   int
	Name string
}

// AddProduct inserts a product and returns the newly created record.
func AddProduct(name string) postgres.Handler[Product] {
	return func(builder postgres.Builder) (Product, error) {
		var product Product
		query := builder(`
			INSERT INTO products (name) VALUES ($1) RETURNING id, name;
		`)
		err := query.Arguments(name).QueryRow(&product.ID, &product.Name)
		return product, err
	}
}

func TestAddProduct(t *testing.T) {
	ctx := context.Background()

	// 1. Create a new mock for pgxpool
	mockPool := mock.NewPGXPoolMock()

	// 2. Initialize Octobe with the mock pool
	db, err := octobe.New(postgres.OpenPGXPoolWithPool(mockPool))
	require.NoError(t, err)

	productName := "Super Gadget"

	// 3. Set up expectations
	rows := mock.NewMockRow(1, productName)
	mockPool.ExpectQueryRow("INSERT INTO products").WithArgs(productName).WillReturnRow(rows)

	// 4. Start a session (it can be non-transactional for this test)
	session, err := db.Begin(ctx)
	require.NoError(t, err)

	// 5. Execute the handler
	product, err := postgres.Execute(session, AddProduct(productName))
	require.NoError(t, err)

	// 6. Assert the results
	require.Equal(t, 1, product.ID)
	require.Equal(t, productName, product.Name)

	// 7. Verify that all expectations were met
	require.NoError(t, mockPool.AllExpectationsMet())
}
```
