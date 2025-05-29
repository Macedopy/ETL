package handlers

import (
	"database/sql"
	"encoding/json"
	"etl_project/etl"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

// DB holds database connections and ETL instance
type DB struct {
	Transacional *sql.DB
	DW           *sql.DB
	ETL          *etl.ETL
}

func NewDB() (*DB, error) {
	user := "postgres"
	password := "postgres"
	if user == "" || password == "" {
		return nil, fmt.Errorf("DB_USER e DB_PASSWORD devem estar definidos")
	}

	connStrTransacional := fmt.Sprintf("user=%s password=%s dbname=ecommerce_ferramentas host=localhost sslmode=disable", user, password)
	connStrDW := fmt.Sprintf("user=%s password=%s dbname=dw_ferramentas host=localhost sslmode=disable", user, password)

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

	etlInstance := etl.NewETL(transacional, dw)
	return &DB{
		Transacional: transacional,
		DW:           dw,
		ETL:          etlInstance,
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

// Request structures
type Cliente struct {
	Nome  string `json:"nome"`
	Email string `json:"email"`
	Senha string `json:"senha"`
}

type Pedido struct {
	ClienteID int              `json:"cliente_id"`
	Itens     []etl.ItemPedido `json:"itens"`
}

// CriarConta handles client creation
func (db *DB) CriarConta(w http.ResponseWriter, r *http.Request) {
	var cliente Cliente
	if err := json.NewDecoder(r.Body).Decode(&cliente); err != nil {
		http.Error(w, "Erro ao decodificar JSON", http.StatusBadRequest)
		return
	}

	id, err := db.ETL.CriarCliente(cliente.Nome, cliente.Email, cliente.Senha)
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao criar cliente: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Cliente criado com ID: %d", id)
}

// Login handles client authentication
func (db *DB) Login(w http.ResponseWriter, r *http.Request) {
	var cliente Cliente
	if err := json.NewDecoder(r.Body).Decode(&cliente); err != nil {
		http.Error(w, "Erro ao decodificar JSON", http.StatusBadRequest)
		return
	}

	id, err := db.ETL.Login(cliente.Email, cliente.Senha)
	if err != nil {
		http.Error(w, "Credenciais inválidas", http.StatusUnauthorized)
		return
	}

	fmt.Fprintf(w, "Login bem-sucedido! Cliente ID: %d", id)
}

// FaturarPedido handles order creation
func (db *DB) FaturarPedido(w http.ResponseWriter, r *http.Request) {
	var pedido Pedido
	if err := json.NewDecoder(r.Body).Decode(&pedido); err != nil {
		http.Error(w, "Erro ao decodificar JSON", http.StatusBadRequest)
		return
	}

	pedidoID, err := db.ETL.FaturarPedido(pedido.ClienteID, pedido.Itens)
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao faturar pedido: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Pedido faturado com ID: %d", pedidoID)
}

// AdicionarFerramenta handles tool addition
func (db *DB) AdicionarFerramenta(w http.ResponseWriter, r *http.Request) {
	var ferramenta etl.Ferramenta
	if err := json.NewDecoder(r.Body).Decode(&ferramenta); err != nil {
		http.Error(w, "Erro ao decodificar JSON", http.StatusBadRequest)
		return
	}

	id, err := db.ETL.AdicionarFerramenta(ferramenta)
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao adicionar ferramenta: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Ferramenta adicionada com ID: %d", id)
}

// ListarFerramentas handles tool listing
func (db *DB) ListarFerramentas(w http.ResponseWriter, r *http.Request) {
	ferramentas, err := db.ETL.ListarFerramentas()
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao listar ferramentas: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ferramentas)
}

// AtualizarFerramenta handles tool updates
func (db *DB) AtualizarFerramenta(w http.ResponseWriter, r *http.Request) {
	var ferramenta etl.Ferramenta
	if err := json.NewDecoder(r.Body).Decode(&ferramenta); err != nil {
		http.Error(w, "Erro ao decodificar JSON", http.StatusBadRequest)
		return
	}

	if ferramenta.ID == 0 {
		http.Error(w, "ID da ferramenta é obrigatório", http.StatusBadRequest)
		return
	}

	err := db.ETL.AtualizarFerramenta(ferramenta)
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao atualizar ferramenta: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Ferramenta com ID %d atualizada", ferramenta.ID)
}
