package etl

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type ETL struct {
	db *sql.DB // Banco transacional
	dw *sql.DB // Banco DW
}

func NewETL(db, dw *sql.DB) *ETL {
	return &ETL{db: db, dw: dw}
}

func (e *ETL) CriarConta(nome, email, senha string) error {
	if nome == "" || email == "" || senha == "" {
		return fmt.Errorf("nome, email e senha são obrigatórios")
	}

	var clienteID int
	err := e.db.QueryRow(
		"INSERT INTO clientes (nome, email, senha) VALUES ($1, $2, $3) RETURNING id",
		nome, email, senha,
	).Scan(&clienteID)
	if err != nil {
		return fmt.Errorf("erro ao criar conta: %v", err)
	}

	itens, _ := json.Marshal(map[string]string{"evento": "conta_criada"})
	_, err = e.dw.Exec(
		"INSERT INTO eventos_faturados (evento_tipo, pedido_id, cliente_nome, cliente_email, data_evento, valor_total, itens) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		"conta_criada", nil, nome, email, time.Now(), 0.0, itens,
	)
	if err != nil {
		return fmt.Errorf("erro ao inserir no DW (conta_criada): %v", err)
	}

	return nil
}

func (e *ETL) Login(clienteID int) error {
	var exists bool
	err := e.db.QueryRow("SELECT EXISTS (SELECT 1 FROM clientes WHERE id = $1)", clienteID).Scan(&exists)
	if err != nil || !exists {
		return fmt.Errorf("cliente com ID %d não encontrado: %v", clienteID, err)
	}

	_, err = e.db.Exec("INSERT INTO logins (cliente_id) VALUES ($1)", clienteID)
	if err != nil {
		return fmt.Errorf("erro ao registrar login: %v", err)
	}

	var nome, email string
	err = e.db.QueryRow("SELECT nome, email FROM clientes WHERE id = $1", clienteID).Scan(&nome, &email)
	if err != nil {
		return fmt.Errorf("erro ao obter cliente: %v", err)
	}

	itens, _ := json.Marshal(map[string]string{"evento": "login"})
	_, err = e.dw.Exec(
		"INSERT INTO eventos_faturados (evento_tipo, pedido_id, cliente_nome, cliente_email, data_evento, valor_total, itens) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		"login", nil, nome, email, time.Now(), 0.0, itens,
	)
	if err != nil {
		return fmt.Errorf("erro ao inserir no DW (login): %v", err)
	}

	return nil
}

func (e *ETL) CriarPedidoCompra(clienteID int) (int, error) {
	var exists bool
	err := e.db.QueryRow("SELECT EXISTS (SELECT 1 FROM clientes WHERE id = $1)", clienteID).Scan(&exists)
	if err != nil || !exists {
		return 0, fmt.Errorf("cliente com ID %d não encontrado: %v", clienteID, err)
	}

	var pedidoID int
	err = e.db.QueryRow(
		"INSERT INTO pedidos (cliente_id, status, valor_total) VALUES ($1, $2, $3) RETURNING id",
		clienteID, "pendente", 0.0,
	).Scan(&pedidoID)
	if err != nil {
		return 0, fmt.Errorf("erro ao criar pedido de compra: %v", err)
	}

	var nome, email string
	err = e.db.QueryRow("SELECT nome, email FROM clientes WHERE id = $1", clienteID).Scan(&nome, &email)
	if err != nil {
		return 0, fmt.Errorf("erro ao obter cliente: %v", err)
	}

	itens, _ := json.Marshal(map[string]string{"evento": "pedido_criado"})
	_, err = e.dw.Exec(
		"INSERT INTO eventos_faturados (evento_tipo, pedido_id, cliente_nome, cliente_email, data_evento, valor_total, itens) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		"pedido_criado", pedidoID, nome, email, time.Now(), 0.0, itens,
	)
	if err != nil {
		return 0, fmt.Errorf("erro ao inserir no DW (pedido_criado): %v", err)
	}

	return pedidoID, nil
}

func (e *ETL) AdicionarItensPedido(pedidoID int, itens []struct {
	ProdutoID  int `json:"produto_id"`
	Quantidade int `json:"quantidade"`
}) error {
	var status string
	err := e.db.QueryRow("SELECT status FROM pedidos WHERE id = $1", pedidoID).Scan(&status)
	if err != nil || status != "pendente" {
		return fmt.Errorf("pedido %d não encontrado ou não está pendente: %v", pedidoID, err)
	}

	tx, err := e.db.Begin()
	if err != nil {
		return fmt.Errorf("erro ao iniciar transação: %v", err)
	}
	defer tx.Rollback()

	var valorTotal float64
	for _, item := range itens {
		var preco float64
		err := e.db.QueryRow("SELECT preco FROM produtos WHERE id = $1", item.ProdutoID).Scan(&preco)
		if err != nil {
			return fmt.Errorf("erro ao obter preço do produto %d: %v", item.ProdutoID, err)
		}
		if item.Quantidade <= 0 {
			return fmt.Errorf("quantidade inválida para produto %d", item.ProdutoID)
		}

		_, err = tx.Exec(
			"INSERT INTO itens_pedido (pedido_id, produto_id, quantidade, preco_unitario) VALUES ($1, $2, $3, $4)",
			pedidoID, item.ProdutoID, item.Quantidade, preco,
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir item do pedido: %v", err)
		}
		valorTotal += preco * float64(item.Quantidade)
	}

	_, err = tx.Exec(
		"UPDATE pedidos SET valor_total = valor_total + $1 WHERE id = $2",
		valorTotal, pedidoID,
	)
	if err != nil {
		return fmt.Errorf("erro ao atualizar valor total do pedido: %v", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("erro ao commitar transação: %v", err)
	}

	var clienteID int
	var nome, email string
	err = e.db.QueryRow("SELECT cliente_id FROM pedidos WHERE id = $1", pedidoID).Scan(&clienteID)
	if err != nil {
		return fmt.Errorf("erro ao obter cliente do pedido: %v", err)
	}
	err = e.db.QueryRow("SELECT nome, email FROM clientes WHERE id = $1", clienteID).Scan(&nome, &email)
	if err != nil {
		return fmt.Errorf("erro ao obter dados do cliente: %v", err)
	}

	rows, err := e.db.Query(
		"SELECT ip.produto_id, ip.quantidade, ip.preco_unitario, p.titulo FROM itens_pedido ip JOIN produtos p ON ip.produto_id = p.id WHERE ip.pedido_id = $1",
		pedidoID,
	)
	if err != nil {
		return fmt.Errorf("erro ao obter itens: %v", err)
	}
	defer rows.Close()

	var itensJSON []map[string]interface{}
	for rows.Next() {
		var produtoID, quantidade int
		var precoUnitario float64
		var titulo string
		if err := rows.Scan(&produtoID, &quantidade, &precoUnitario, &titulo); err != nil {
			return fmt.Errorf("erro ao scanear itens: %v", err)
		}
		itensJSON = append(itensJSON, map[string]interface{}{
			"produto_id":     produtoID,
			"quantidade":     quantidade,
			"preco_unitario": precoUnitario,
			"titulo":         titulo,
		})
	}

	itensBytes, _ := json.Marshal(itensJSON)
	_, err = e.dw.Exec(
		"INSERT INTO eventos_faturados (evento_tipo, pedido_id, cliente_nome, cliente_email, data_evento, valor_total, itens) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		"itens_adicionados", pedidoID, nome, email, time.Now(), valorTotal, itensBytes,
	)
	if err != nil {
		return fmt.Errorf("erro ao inserir no DW (itens_adicionados): %v", err)
	}

	return nil
}

func (e *ETL) FaturarPedido(pedidoID int) error {
	var status string
	var clienteID int
	var valorTotal float64
	err := e.db.QueryRow("SELECT status, cliente_id, valor_total FROM pedidos WHERE id = $1", pedidoID).Scan(&status, &clienteID, &valorTotal)
	if err != nil || status != "pendente" {
		return fmt.Errorf("pedido %d não encontrado ou não está pendente: %v", pedidoID, err)
	}

	_, err = e.db.Exec("UPDATE pedidos SET status = 'faturado' WHERE id = $1", pedidoID)
	if err != nil {
		return fmt.Errorf("erro ao faturar pedido: %v", err)
	}

	var nome, email string
	err = e.db.QueryRow("SELECT nome, email FROM clientes WHERE id = $1", clienteID).Scan(&nome, &email)
	if err != nil {
		return fmt.Errorf("erro ao obter cliente: %v", err)
	}

	rows, err := e.db.Query(
		"SELECT ip.produto_id, ip.quantidade, ip.preco_unitario, p.titulo FROM itens_pedido ip JOIN produtos p ON ip.produto_id = p.id WHERE ip.pedido_id = $1",
		pedidoID,
	)
	if err != nil {
		return fmt.Errorf("erro ao obter itens: %v", err)
	}
	defer rows.Close()

	var itensJSON []map[string]interface{}
	for rows.Next() {
		var produtoID, quantidade int
		var precoUnitario float64
		var titulo string
		if err := rows.Scan(&produtoID, &quantidade, &precoUnitario, &titulo); err != nil {
			return fmt.Errorf("erro ao scanear itens: %v", err)
		}
		itensJSON = append(itensJSON, map[string]interface{}{
			"produto_id":     produtoID,
			"quantidade":     quantidade,
			"preco_unitario": precoUnitario,
			"titulo":         titulo,
		})
	}

	itensBytes, _ := json.Marshal(itensJSON)
	_, err = e.dw.Exec(
		"INSERT INTO eventos_faturados (evento_tipo, pedido_id, cliente_nome, cliente_email, data_evento, valor_total, itens) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		"pedido_faturado", pedidoID, nome, email, time.Now(), valorTotal, itensBytes,
	)
	if err != nil {
		return fmt.Errorf("erro ao inserir no DW (pedido_faturado): %v", err)
	}

	return nil
}
