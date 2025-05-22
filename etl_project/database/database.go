package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

type DB struct {
	Transacional *sql.DB
	DW           *sql.DB
}

func NewDB() (*DB, error) {
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	if user == "" || password == "" {
		return nil, fmt.Errorf("DB_USER e DB_PASSWORD devem estar definidos")
	}

	connStrTransacional := fmt.Sprintf("user=%s password=%s dbname=ecommerce_livraria host=localhost sslmode=disable", user, password)
	connStrDW := fmt.Sprintf("user=%s password=%s dbname=dw_livraria host=localhost sslmode=disable", user, password)

	transacional, err := sql.Open("postgres", connStrTransacional)
	if err != nil {
		return nil, fmt.Errorf("erro ao conectar ao banco transacional: %v", err)
	}
	if err := transacional.Ping(); err != nil {
		return nil, fmt.Errorf("erro ao pingar banco transacional: %v", err)
	}

	dw, err := sql.Open("postgres", connStrDW)
	if err != nil {
		return nil, fmt.Errorf("erro ao conectar ao DW: %v", err)
	}
	if err := dw.Ping(); err != nil {
		return nil, fmt.Errorf("erro ao pingar DW: %v", err)
	}

	return &DB{
		Transacional: transacional,
		DW:           dw,
	}, nil
}

func (db *DB) Close() {
	if err := db.Transacional.Close(); err != nil {
		log.Printf("Erro ao fechar conexão transacional: %v", err)
	}
	if err := db.DW.Close(); err != nil {
		log.Printf("Erro ao fechar conexão DW: %v", err)
	}
}
