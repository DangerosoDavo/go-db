// Path: connections.go
package godb

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	// Import the mysql driver
	_ "github.com/go-sql-driver/mysql"
)

var (
	// ErrNoConnections is returned when there are no connections available
	ErrNoConnections = errors.New("no connections available")
)

// ConnectionPool is a pool of connections to a database
type ConnectionPool struct {
	mu              sync.Mutex
	connections     []*Connection
	connectionCount int
	ConnType        ConnType
	WaitChannel     chan bool
	Waiting         bool
	NumWaiting      int
	WaitMutex       sync.Mutex
}

// NewConnectionPool creates a new connection pool for a given set of DSNs
func NewConnectionPool(connType ConnType, dsn ...string) (*ConnectionPool, error) {
	pool := &ConnectionPool{
		connections:     make([]*Connection, 0, len(dsn)),
		connectionCount: len(dsn),
		ConnType:        connType,
		WaitChannel:     make(chan bool),
		WaitMutex:       sync.Mutex{},
	}

	for _, dsn := range dsn {
		conn, err := pool.newConnection(dsn, connType)
		if err != nil {
			return nil, err
		}

		pool.connections = append(pool.connections, conn)
	}

	return pool, nil
}

func (p *ConnectionPool) newConnection(dsn string, connType ConnType) (*Connection, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return NewConnection(connType, db), nil
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.connections {
		conn.Close()
	}

	p.WaitMutex.Lock()
	defer p.WaitMutex.Unlock()
	if p.Waiting {
		for i := 0; i < len(p.WaitChannel); i++ {
			p.WaitChannel <- false
		}
		close(p.WaitChannel)
		p.Waiting = false
	}

	return nil
}

// Ping pings all connections in the pool
func (p *ConnectionPool) Ping() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.connections {
		if err := conn.Ping(); err != nil {
			return err
		}
	}

	return nil
}

// QueryContext runs a query on a connection in the pool
func (p *ConnectionPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	conn, err := p.getConn()
	if err != nil {
		return nil, err
	}
	defer p.putConn(conn)

	return conn.QueryContext(ctx, query, args...)
}

// ExecContext runs an exec on a connection in the pool
func (p *ConnectionPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	conn, err := p.getConn()
	if err != nil {
		return nil, err
	}
	defer p.putConn(conn)

	return conn.ExecContext(ctx, query, args...)
}

// Query runs a query on a connection in the pool
func (p *ConnectionPool) Query(query string, args ...interface{}) (*sql.Rows, error) {
	conn, err := p.getConn()
	if err != nil {
		return nil, err
	}
	defer p.putConn(conn)

	return conn.Query(query, args...)
}

// Exec runs an exec on a connection in the pool
func (p *ConnectionPool) Exec(query string, args ...interface{}) (sql.Result, error) {
	conn, err := p.getConn()
	if err != nil {
		return nil, err
	}
	defer p.putConn(conn)

	return conn.Exec(query, args...)
}

func (p *ConnectionPool) getConn() (*Connection, error) {
	p.mu.Lock()

	if len(p.connections) == 0 {
		p.mu.Unlock()
		wait := p.waitConnection()
		result := <-wait
		if !result {
			return nil, ErrNoConnections
		}
		p.mu.Lock()
	}

	defer p.mu.Unlock()

	conn := p.connections[0]
	p.connections = p.connections[1:]

	return conn, nil
}

func (p *ConnectionPool) putConn(conn *Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.connections = append(p.connections, conn)

	p.WaitMutex.Lock()
	defer p.WaitMutex.Unlock()
	if p.Waiting {
		p.WaitChannel <- true
		p.NumWaiting -= 1
	}
}

func (p *ConnectionPool) GetConnType() ConnType {
	return p.ConnType
}

func (p *ConnectionPool) waitConnection() chan bool {
	p.WaitMutex.Lock()
	defer p.WaitMutex.Unlock()

	p.NumWaiting += 1
	if !p.Waiting {
		p.WaitChannel = make(chan bool)
		p.Waiting = true
		return p.WaitChannel
	}

	return p.WaitChannel
}

func (g *GoDB) GetConnectionPool(connType ConnType) *ConnectionPool {
	switch connType {
	case GetReadConn:
		return g.ReadPool
	case GetWriteConn:
		return g.WritePool
	case GetReadWriteConn:
		return g.WritePool
	}
	return nil
}

func (p *ConnectionPool) QueryRow(query string) *sql.Row {
	conn, err := p.getConn()
	if err != nil {
		return nil
	}
	defer p.putConn(conn)

	return conn.QueryRow(query)
}
