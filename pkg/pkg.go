package pkg

import (
	"context"
	"database/sql"
	"net/url"
	"os"

	"github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// NewMongoDBConnection establishes a connection to a MongoDB server using the provided URI.
// The function parses the URI and checks its validity, then attempts to establish a connection.
// If successful, it returns the MongoDB client to interact with the database.
// If any error occurs, it logs the error and terminates the application.
func NewMongoDBConnection(connectionURI string) *mongo.Client {
	parsedURL, err := url.Parse(connectionURI)
	if err != nil {
		logrus.Fatalf("Invalid URL Format: %v", err.Error())
	}

	if parsedURL.Scheme != "mongodb" && parsedURL.Scheme != "mongodb+srv" {
		logrus.Fatalf("Invalid scheme: %v. Expected 'mongodb' or 'mongodb+srv'", parsedURL.Scheme)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(connectionURI))
	if err != nil {
		logrus.Fatalf("Failed to open new mongodb client with provided url %v feel free to try again.", err.Error())
	}

	logrus.Info("trying to ping to the database")

	err = client.Ping(context.Background(), nil)
	if err != nil {
		logrus.Fatalf("Database connection wasnt successful failed to pinging to client err: %v", err.Error())
	}

	logrus.Info("Successfully Connected to the database")

	return client
}

// NewSQLDBConnection establishes a connection to a MySQL database using the provided configuration.
// It accepts either a connection string or a MySQL config object. After establishing the connection, it pings the database.
// If successful, it returns the SQL database connection to interact with the database.
// If any error occurs, it logs the error and terminates the application.
func NewSQLDBConnection[T string | mysql.Config](cfg T) *sql.DB {
	var dsn string

	switch v := any(cfg).(type) {
	case string:
		dsn = v
	case mysql.Config:
		dsn = v.FormatDSN()
	default:
		logrus.Fatalf("Invalid config type: %T", v)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logrus.Fatalf("Failed to open database connection: %v", err.Error())
	}

	logrus.Info("Trying to ping the database")
	err = db.Ping()
	if err != nil {
		logrus.Fatalf("Failed to ping database: %v", err.Error())
	}

	logrus.Info("Successfully connected to the SQL database")
	return db
}

// NewPostgresDBConnection establishes a connection to a PostgreSQL database using the provided connection string.
// It attempts to ping the database and logs the result. If successful, it returns the SQL database connection.
// If any error occurs, it logs the error and terminates the application.
func NewPostgresDBConnection[T string](cfg T) *sql.DB {
	dsn := string(cfg)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logrus.Fatalf("Failed to open database connection: %v", err.Error())
	}

	logrus.Info("Trying to ping the database")
	err = db.Ping()
	if err != nil {
		logrus.Fatalf("Failed to ping database: %v", err.Error())
	}

	logrus.Info("Successfully connected to the PostgreSQL database")
	return db
}

// NewRedisConnection establishes a connection to a Redis server using the provided configuration.
// It accepts either a connection string or a Redis config object. After establishing the connection, it pings the server.
// If successful, it returns the Redis client to interact with the database.
// If any error occurs, it logs the error and terminates the application.
func NewRedisConnection[T string | *redis.Options](cfg T) *redis.Client {
	var client *redis.Client

	switch v := any(cfg).(type) {
	case string:
		client = redis.NewClient(&redis.Options{
			Addr: v,
		})
	case *redis.Options:

		client = redis.NewClient(v)
	default:
		logrus.Fatalf("Invalid config type: %T", v)
	}

	logrus.Info("Trying to ping the Redis server")
	err := client.Ping(context.Background()).Err()
	if err != nil {
		logrus.Fatalf("Failed to connect to Redis: %v", err.Error())
	}

	logrus.Info("Successfully connected to Redis")
	return client
}

// NewSQLiteConnection establishes a connection to an SQLite database. It accepts either a connection string or a file path.
// If the file path is provided, it will create the SQLite database file if it doesn't exist.
// The function attempts to open the SQLite database and ping it to ensure the connection is successful.
// If successful, it returns the SQL database connection.
// If any error occurs, it logs the error and terminates the application.
func NewSQLiteConnection[T string](cfg, filePath T) *sql.DB {
	var dsn string

	if cfg != "" {
		dsn = string(cfg)
	} else {
		if filePath != "" {
			if _, err := os.Stat(string(filePath)); os.IsNotExist(err) {
				logrus.Infof("SQLite database file does not exist, creating new database at %v", filePath)

				file, err := os.Create(string(filePath))
				if err != nil {
					logrus.Fatalf("Failed to create SQLite database file: %v", err.Error())
				}
				file.Close()
			}
			dsn = "file:" + string(filePath) + "?cache=shared&mode=rwc"
		} else {
			logrus.Fatalf("Both connection string and file path are empty. Cannot connect to SQLite.")
		}
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		logrus.Fatalf("Failed to open SQLite database connection: %v", err.Error())
	}

	logrus.Info("Trying to ping the SQLite database")
	err = db.Ping()
	if err != nil {
		logrus.Fatalf("Failed to ping SQLite database: %v", err.Error())
	}

	logrus.Info("Successfully connected to SQLite database")
	return db
}

/*
func NewCassandraConnection(connectionURI string) *gocql.Session {
	cluster := gocql.NewCluster(connectionURI)
	cluster.Timeout = 1000
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		logrus.Fatalf("Failed to connect to Cassandra: %v", err.Error())
	}

	logrus.Info("Successfully connected to Cassandra")
	return session
}
*/
