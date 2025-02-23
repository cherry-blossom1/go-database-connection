package main

import (
	"context"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestNewMongoDBConnection(t *testing.T) {
	client := NewMongoDBConnection("mongodb://localhost:27017")

	assert.NotNil(t, client)

	err := client.Ping(context.Background(), nil)

	assert.NoError(t, err, "Expected No Error when pinging MongoDB")
}

func TestNewSQLDBConnection(t *testing.T) {
	dsn := "root:password@tcp(localhost:3306)/testdb"

	db := NewSQLDBConnection(dsn)

	assert.NotNil(t, db)

	err := db.Ping()
	assert.NoError(t, err, "Expected no error when pinging MySQL")
}

func TestSQLConnectionWithProperDSNConfigs(t *testing.T) {
	cfg := mysql.Config{
		Addr:                 "localhost:3306",
		User:                 "root",
		Passwd:               "password",
		AllowNativePasswords: true,
		Net:                  "tcp",
		ParseTime:            true,
		DBName:               "test2db",
	}

	db := NewSQLDBConnection(cfg)

	assert.NotNil(t, db)

	err := db.Ping()
	assert.NoError(t, err, "Expected no error when pinging MySQL")
}

func TestNewPostgresDBConnection(t *testing.T) {
	dsn := "postgres://postgres:password@localhost:5432/testdb?sslmode=disable"

	db := NewPostgresDBConnection(dsn)

	assert.NotNil(t, db)

	err := db.Ping()
	assert.NoError(t, err, "Expected no error when pinging PostgreSQL")
}

func TestNewRedisConnection(t *testing.T) {
	address := "localhost:6379"

	client := NewRedisConnection(address)

	assert.NotNil(t, client)

	err := client.Ping(context.Background()).Err()
	assert.NoError(t, err, "Expected no error when pinging Redis")
}

func TestRedisConnectionWithRedisOptions(t *testing.T) {
	options := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	}

	client := NewRedisConnection(options)

	assert.NotNil(t, client)

	err := client.Ping(context.Background()).Err()

	assert.NoError(t, err, "Expected no error when pinging Redis")
}

func TestNewSQLiteConnection(t *testing.T) {
	filePath := "test.db"

	db := NewSQLiteConnection("", filePath)

	assert.NotNil(t, db)

	err := db.Ping()
	assert.NoError(t, err, "Expected no error when pinging SQLite")
}

/*
func TestNewCassandraConnection(t *testing.T) {
	uri := "localhost:9042"

	session := NewCassandraConnection(uri)

	assert.NotNil(t, session)

	err := session.Query("SELECT * FROM system.local").Exec()
	assert.NoError(t, err, "Expected no error when executing query on Cassandra")
}
*/

// TODO: fix and add more robust testing system for Cassandra database
