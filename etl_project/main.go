package main

import (
	"etl_project/handlers"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	db, err := handlers.NewDB()
	if err != nil {
		log.Fatalf("Erro ao conectar aos bancos: %v", err)
	}
	defer db.Close()

	r := mux.NewRouter()
	r.HandleFunc("/criar-conta", db.CriarConta).Methods("POST")
	r.HandleFunc("/login", db.Login).Methods("POST")
	r.HandleFunc("/faturar-pedido", db.FaturarPedido).Methods("POST")
	r.HandleFunc("/adicionar-ferramenta", db.AdicionarFerramenta).Methods("POST")
	r.HandleFunc("/listar-ferramentas", db.ListarFerramentas).Methods("GET")
	r.HandleFunc("/atualizar-ferramenta", db.AtualizarFerramenta).Methods("PUT")

	log.Println("Servidor rodando na porta 8080...")
	log.Fatal(http.ListenAndServe(":8080", r))
}
