# ![Octobe Logotype](https://raw.github.com/ponrove/octobe/master/doc/octobe_logo.svg)

[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/0d33b2e3bd9d410c949845214cb81e3e)](https://app.codacy.com/gh/ponrove/octobe/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_coverage)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/0d33b2e3bd9d410c949845214cb81e3e)](https://app.codacy.com/gh/ponrove/octobe/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![GoDoc](https://pkg.go.dev/badge/github.com/ponrove/octobe.svg)](https://pkg.go.dev/github.com/ponrove/octobe)
![MIT License](https://img.shields.io/github/license/ponrove/octobe)
![Tag](https://img.shields.io/github/v/tag/ponrove/octobe)
![Version](https://img.shields.io/github/go-mod/go-version/ponrove/octobe)

Octobe is a lean Go package for developers who love writing raw SQL but want to eliminate boilerplate and introduce a
consistent structure to their database code.

The core of Octobe is a handler-based pattern, inspired by Go's `http.Handler`. You write small, focused functions
(`Handlers`) that encapsulate your SQL queries. Octobe takes care of session and transaction management, allowing you to
build a predictable and clean database layer. This approach gives you the freedom of raw SQL with the safety of a
structured framework.

A key advantage is the consistent API across different database drivers. Whether you're working with PostgreSQL or
ClickHouse, the way you structure your code remains the same.

## Key Features

- **Minimalist API**: A small, intuitive API that gets out of your way.
- **Boilerplate Reduction**: Simplifies transaction and session management with `StartTransaction`.
- **Handler Pattern**: Organizes your SQL logic into clean, reusable, and testable units.
- **Driver Agnostic**: Provides a consistent developer experience for different databases.

## Usage

The primary way to interact with the database is through `StartTransaction`. This function handles the entire lifecycle
of a database transaction: it begins the transaction, executes your code, and automatically commits on success or rolls
back on error or panic.

### Postgres Example (using `pgxpool`)

This example demonstrates setting up a products table, inserting a new product, and retrieving it, all within a single
transaction.

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/ponrove/octobe"
	"github.com/ponrove/octobe/driver/postgres"
)

// Product is a model that represents a product in the database.
type Product struct {
	ID   int
	Name string
}

// CreateProductsTable creates the necessary table if it doesn't exist.
func CreateProductsTable() postgres.Handler[octobe.Void] {
	return func(builder postgres.Builder) (octobe.Void, error) {
		query := builder(`
			CREATE TABLE IF NOT EXISTS products (
				id   SERIAL PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		_, err := query.Exec()
		return nil, err
	}
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

// ProductByName selects a product from the database by its name.
func ProductByName(name string) postgres.Handler[Product] {
	return func(builder postgres.Builder) (Product, error) {
		var product Product
		query := builder(`
			SELECT id, name FROM products WHERE name = $1;
		`)
		err := query.Arguments(name).QueryRow(&product.ID, &product.Name)
		return product, err
	}
}

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DSN")
	if dsn == "" {
		panic("DSN environment variable is not set")
	}

	// 1. Initialize Octobe with a Postgres driver (pgxpool in this case).
	db, err := octobe.New(postgres.OpenPGXPool(ctx, dsn))
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	// 2. Use StartTransaction to manage a transactional block of work.
	// It automatically handles begin, commit, and rollback.
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		// Create table if it doesn't exist.
		if _, err := postgres.Execute(session, CreateProductsTable()); err != nil {
			return fmt.Errorf("failed to create products table: %w", err)
		}

		productName := uuid.New().String()

		// Insert a new product.
		product1, err := postgres.Execute(session, AddProduct(productName))
		if err != nil {
			return fmt.Errorf("failed to add product: %w", err)
		}
		fmt.Printf("Inserted product: ID=%d, Name=%s\n", product1.ID, product1.Name)

		// Retrieve the same product.
		product2, err := postgres.Execute(session, ProductByName(productName))
		if err != nil {
			return fmt.Errorf("failed to get product by name: %w", err)
		}
		fmt.Printf("Retrieved product: ID=%d, Name=%s\n", product2.ID, product2.Name)

		return nil
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{})) // Specify transaction options.

	if err != nil {
		panic(err)
	}
}
```

### ClickHouse Example

Octobe provides a consistent API even for databases like ClickHouse that don't support traditional transactions. The `StartTransaction` function still works, but `Commit` and `Rollback` are no-ops, allowing you to structure your code identically.

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ponrove/octobe"
	ch "github.com/ponrove/octobe/driver/clickhouse" // Using 'ch' as an alias
)

// Event represents a record in our ClickHouse table.
type Event struct {
	ID        uint64
	Message   string
	Timestamp time.Time
}

// CreateEventsTable creates a sample table in ClickHouse.
func CreateEventsTable() ch.Handler[octobe.Void] {
	return func(builder ch.Builder) (octobe.Void, error) {
		query := builder(`
			CREATE TABLE IF NOT EXISTS events (
				id         UInt64,
				message    String,
				timestamp  DateTime
			) ENGINE = MergeTree() ORDER BY id;
		`)
		err := query.Exec()
		return nil, err
	}
}

// InsertEvent adds a new event to the table.
func InsertEvent(event Event) ch.Handler[octobe.Void] {
	return func(builder ch.Builder) (octobe.Void, error) {
		query := builder(`
			INSERT INTO events (id, message, timestamp) VALUES (?, ?, ?);
		`)
		err := query.Arguments(event.ID, event.Message, event.Timestamp).Exec()
		return nil, err
	}
}

// CountEvents returns the total number of events in the table.
func CountEvents() ch.Handler[int64] {
	return func(builder ch.Builder) (int64, error) {
		var count int64
		query := builder(`
			SELECT count() FROM events;
		`)
		err := query.QueryRow(&count)
		return count, err
	}
}

func main() {
	ctx := context.Background()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		panic("CLICKHOUSE_ADDR environment variable is not set")
	}

	// 1. Initialize Octobe with the ClickHouse native driver.
	opts := &clickhouse.Options{Addr: []string{addr}}
	db, err := octobe.New(ch.OpenNative(opts))
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	// 2. Use StartTransaction for consistent code structure.
	// For ClickHouse, Commit/Rollback are no-ops, but the pattern remains.
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[ch.Builder]) error {
		// Create table if it doesn't exist.
		if _, err := ch.Execute(session, CreateEventsTable()); err != nil {
			return fmt.Errorf("failed to create events table: %w", err)
		}

		// Insert a new event.
		event := Event{ID: 1, Message: "hello world", Timestamp: time.Now()}
		if _, err := ch.Execute(session, InsertEvent(event)); err != nil {
			return fmt.Errorf("failed to insert event: %w", err)
		}
		fmt.Println("Inserted event successfully.")

		// Count the events in the table.
		count, err := ch.Execute(session, CountEvents())
		if err != nil {
			return fmt.Errorf("failed to count events: %w", err)
		}
		fmt.Printf("Total events in table: %d\n", count)

		return nil
	})

	if err != nil {
		panic(err)
	}
}
```

## Contributing

Contributions are welcome! Please feel free to open a pull request with any improvements, bug fixes, or new features.

1.  Fork the repository.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

Please ensure your code is well-tested and follows Go best practices.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
