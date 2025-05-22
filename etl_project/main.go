package main

import (
	"etl_project/database"
	"etl_project/etl"
	"flag"
	"log"
)

func main() {
	dbConn, err := database.NewDB()
	if err != nil {
		log.Fatalf("Erro ao conectar aos bancos: %v", err)
	}
	defer dbConn.Close()

	etl := etl.NewETL(dbConn.Transacional, dbConn.DW)

	criarConta := flag.Bool("criar-conta", false, "Executar ETL para criar conta")
	login := flag.Bool("login", false, "Executar ETL para login")
	criarPedido := flag.Bool("criar-pedido", false, "Executar ETL para criar pedido de compra")
	adicionarItens := flag.Bool("adicionar-itens", false, "Executar ETL para adicionar itens ao pedido")
	faturarPedido := flag.Bool("faturar-pedido", false, "Executar ETL para faturar pedido")
	clienteID := flag.Int("cliente-id", 1, "ID do cliente")
	pedidoID := flag.Int("pedido-id", 1, "ID do pedido")
	nome := flag.String("nome", "Ana Costa", "Nome do cliente")
	email := flag.String("email", "ana@email.com", "Email do cliente")
	senha := flag.String("senha", "senha789", "Senha do cliente")
	flag.Parse()

	if *criarConta {
		err = etl.CriarConta(*nome, *email, *senha)
		if err != nil {
			log.Printf("Erro ao criar conta: %v", err)
		} else {
			log.Println("Conta criada com sucesso")
		}
	} else if *login {
		err = etl.Login(*clienteID)
		if err != nil {
			log.Printf("Erro ao fazer login: %v", err)
		} else {
			log.Println("Login realizado com sucesso")
		}
	} else if *criarPedido {
		pedidoID, err := etl.CriarPedidoCompra(*clienteID)
		if err != nil {
			log.Printf("Erro ao criar pedido de compra: %v", err)
		} else {
			log.Printf("Pedido de compra criado com sucesso, ID: %d", pedidoID)
		}
	} else if *adicionarItens {
		itens := []struct {
			ProdutoID  int `json:"produto_id"`
			Quantidade int `json:"quantidade"`
		}{
			{ProdutoID: 1, Quantidade: 1},
			{ProdutoID: 2, Quantidade: 1},
		}
		err = etl.AdicionarItensPedido(*pedidoID, itens)
		if err != nil {
			log.Printf("Erro ao adicionar itens ao pedido: %v", err)
		} else {
			log.Println("Itens adicionados ao pedido com sucesso")
		}
	} else if *faturarPedido {
		err = etl.FaturarPedido(*pedidoID)
		if err != nil {
			log.Printf("Erro ao faturar pedido: %v", err)
		} else {
			log.Println("Pedido faturado com sucesso")
		}
	} else {
		log.Println("Nenhuma rota selecionada. Use: -criar-conta, -login, -criar-pedido, -adicionar-itens ou -faturar-pedido")
	}
}
