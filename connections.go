package godb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
)

// This file contains the code for the database connection pool.
// It should handle master/slave connections.
// It should be able to asynchronously handle database requests.

type ConnType int

const (
	ConnTypeReadWrite ConnType = iota
	ConnTypeReadOnly
	ConnTypeWriteOnly
)

const (
	GetReadConn = iota
	GetWriteConn
	GetReadWriteConn
)

// String returns the string representation of the connection type
func (c ConnType) String() string {
	switch c {
	case ConnTypeReadOnly:
		return "read"
	case ConnTypeWriteOnly:
		return "write"
	case ConnTypeReadWrite:
		return "read/write"
	default:
		return "unknown"
	}
}

var (
	ErrReadOnWriteOnlyConnection = errors.New("read on write only connection") // Returned when a read operation is attempted on a write only connection
	ErrWriteOnReadOnlyConnection = errors.New("write on read only connection") // Returned when a write operation is attempted on a read only connection
)

type Connection struct {
	*sql.DB
	ConnType ConnType
	DSN      string
}

func NewConnection(connType ConnType, db *sql.DB) *Connection {
	return &Connection{
		DB:       db,
		ConnType: connType,
	}
}

func (c *Connection) Close() error {
	return c.DB.Close()
}

func (c *Connection) Ping() error {
	return c.DB.Ping()
}

func (c *Connection) Query(query string, args ...interface{}) (*sql.Rows, error) {
	switch c.ConnType {
	case ConnTypeReadOnly:
		if !isSelectQuery(query) {
			return nil, ErrWriteOnReadOnlyConnection
		}
	case ConnTypeWriteOnly:
		return nil, ErrReadOnWriteOnlyConnection
	}

	return c.DB.Query(query, args...)
}

// Exec executes a query without returning any rows. The args are for any placeholder parameters in the query. If more than one placeholder parameter exists, the args are for the placeholder parameters following the first one. For example: Exec("INSERT INTO foo VALUES(?, ?)", 1, 2) Exec("INSERT INTO foo VALUES(:1, :2)", 1, 2) Exec("INSERT INTO foo VALUES($1, $2)", 1, 2) See the package documentation for examples of placeholder parameter syntax.   On a Read Connection, this will return an error if the query is a SELECT query. On a Write Connection, this will return an error if the query is not a SELECT query.
func (c *Connection) Exec(query string, args ...interface{}) (sql.Result, error) {
	switch c.ConnType {
	case ConnTypeReadOnly:
		return nil, ErrWriteOnReadOnlyConnection
	case ConnTypeWriteOnly:
		if isSelectQuery(query) {
			return nil, ErrReadOnWriteOnlyConnection
		}
	}

	return c.DB.Exec(query, args...)
}

func (c *Connection) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch c.ConnType {
	case ConnTypeReadOnly:
		if !isSelectQuery(query) {
			return nil, ErrWriteOnReadOnlyConnection
		}
	case ConnTypeWriteOnly:
		return nil, ErrReadOnWriteOnlyConnection
	}

	return c.DB.QueryContext(ctx, query, args...)
}

func (c *Connection) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch c.ConnType {
	case ConnTypeReadOnly:
		return nil, ErrWriteOnReadOnlyConnection
	case ConnTypeWriteOnly:
		if isSelectQuery(query) {
			return nil, ErrReadOnWriteOnlyConnection
		}
	}

	return c.DB.ExecContext(ctx, query, args...)
}

func (c *Connection) Begin() (*sql.Tx, error) {
	return c.DB.Begin()
}

func (c *Connection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.DB.BeginTx(ctx, opts)
}

func (c *Connection) Prepare(query string) (*sql.Stmt, error) {
	return c.DB.Prepare(query)
}

func (c *Connection) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.DB.PrepareContext(ctx, query)
}

func (c *Connection) SetMaxIdleConns(n int) {
	c.DB.SetMaxIdleConns(n)
}

func (c *Connection) SetMaxOpenConns(n int) {
	c.DB.SetMaxOpenConns(n)
}

func (c *Connection) SetConnMaxLifetime(dur time.Duration) {
	c.DB.SetConnMaxLifetime(dur)
}

func (c *Connection) Stats() sql.DBStats {
	return c.DB.Stats()
}

func (c *Connection) Driver() driver.Driver {
	return c.DB.Driver()
}

func (c *Connection) Conn(ctx context.Context) (*sql.Conn, error) {
	return c.DB.Conn(ctx)
}

func (c *Connection) SetConnMaxIdleTime(dur time.Duration) {
	c.DB.SetConnMaxIdleTime(dur)
}

func isSelectQuery(query string) bool {
	return len(query) >= 6 && query[:6] == "SELECT"
}

func (g *GoDB) AddConnections(dsn []string, connType ConnType, overwriteExisting bool) error {
	pool, err := NewConnectionPool(connType, dsn...)

	switch connType {
	case ConnTypeReadOnly:
		if g.ReadPool != nil && !overwriteExisting {
			return errors.New("read pool already exists")
		}
		g.ReadPool = pool
	case ConnTypeWriteOnly:
		if g.WritePool != nil && !overwriteExisting {
			return errors.New("write pool already exists")
		}
		g.WritePool = pool
	case ConnTypeReadWrite:
		if g.ReadPool == nil || overwriteExisting {
			g.ReadPool = pool
		}
		if g.WritePool == nil || overwriteExisting {
			g.WritePool = pool
		}
	}

	return err
}
