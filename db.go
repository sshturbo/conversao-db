package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// OpenDB abre a conexão com o banco de dados MySQL
func OpenDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// Testa a conexão
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
