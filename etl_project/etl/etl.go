package etl

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Ferramenta represents a tool in the system
type Ferramenta struct {
	ID        int     `json:"id"`
	Nome      string  `json:"nome"`
	Descricao string  `json:"descricao"`
	Preco     float64 `json:"preco"`
	Estoque   int     `json:"estoque"`
	Categoria string  `json:"categoria"`
	Material  string  `json:"material"`
	Marca     string  `json:"marca"`
	Dimensoes string  `json:"dimensoes"`
}

// ItemPedido represents an item in an order
type ItemPedido struct {
	ProdutoID  int     `json:"produto_id"`
	Quantidade int     `json:"quantidade"`
	Preco      float64 `json:"preco"`
}

// ETL contains the business logic and ETL processes
type ETL struct {
	Transacional *sql.DB
	DW           *sql.DB
}

func NewETL(transacional, dw *sql.DB) *ETL {
	return &ETL{
		Transacional: transacional,
		DW:           dw,
	}
}

// CriarCliente creates a new client in the transactional database
func (e *ETL) CriarCliente(nome, email, senha string) (int, error) {
	query := `INSERT INTO clientes (nome, email, senha) VALUES ($1, $2, $3) RETURNING id`
	var id int
	err := e.Transacional.QueryRow(query, nome, email, senha).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("erro ao criar cliente: %v", err)
	}
	return id, nil
}

// Login verifies client credentials
func (e *ETL) Login(email, senha string) (int, error) {
	var id int
	query := `SELECT id FROM clientes WHERE email = $1 AND senha = $2`
	err := e.Transacional.QueryRow(query, email, senha).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("erro ao verificar credenciais: %v", err)
	}
	return id, nil
}

// FaturarPedido creates an order and triggers ETL
func (e *ETL) FaturarPedido(clienteID int, itens []ItemPedido) (int, error) {
	startTime := time.Now()
	tx, err := e.Transacional.Begin()
	if err != nil {
		return 0, fmt.Errorf("erro ao iniciar transação: %v", err)
	}

	// Validate stock
	for _, item := range itens {
		var estoque int
		query := `SELECT estoque FROM produtos WHERE id = $1`
		err := e.Transacional.QueryRow(query, item.ProdutoID).Scan(&estoque)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("erro ao verificar estoque do produto %d: %v", item.ProdutoID, err)
		}
		if estoque < item.Quantidade {
			tx.Rollback()
			return 0, fmt.Errorf("estoque insuficiente para o produto %d: disponível %d, solicitado %d", item.ProdutoID, estoque, item.Quantidade)
		}
	}

	// Create order
	var pedidoID int
	total := 0.0
	for _, item := range itens {
		total += float64(item.Quantidade) * item.Preco
	}
	tempoProcessamento := time.Since(startTime)
	query := `INSERT INTO pedidos (cliente_id, status, total, tempo_processamento) VALUES ($1, 'FATURADO', $2, $3) RETURNING id`
	err = tx.QueryRow(query, clienteID, total, tempoProcessamento).Scan(&pedidoID)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("erro ao criar pedido: %v", err)
	}

	// Insert order items and update stock
	for _, item := range itens {
		query := `INSERT INTO itens_pedido (pedido_id, produto_id, quantidade, preco_unitario) VALUES ($1, $2, $3, $4)`
		_, err := tx.Exec(query, pedidoID, item.ProdutoID, item.Quantidade, item.Preco)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("erro ao inserir item: %v", err)
		}

		query = `UPDATE produtos SET estoque = estoque - $1 WHERE id = $2`
		_, err = tx.Exec(query, item.Quantidade, item.ProdutoID)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("erro ao atualizar estoque do produto %d: %v", item.ProdutoID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("erro ao commitar transação: %v", err)
	}

	if err := e.executarETLPedido(pedidoID); err != nil {
		log.Printf("Erro no ETL de pedido: %v", err)
	}

	return pedidoID, nil
}

// executarETLPedido consolidates order data in the DW
func (e *ETL) executarETLPedido(pedidoID int) error {
	query := `
        SELECT p.id, p.cliente_id, c.nome, c.email, c.data_criacao, p.data_pedido, p.total, p.tempo_processamento,
               json_agg(json_build_object(
                   'produto_id', ip.produto_id,
                   'nome', pr.nome,
                   'quantidade', ip.quantidade,
                   'preco_unitario', ip.preco_unitario,
                   'categoria', pr.categoria,
                   'material', pr.material,
                   'marca', pr.marca,
                   'dimensoes', pr.dimensoes
               )) as itens
        FROM pedidos p
        JOIN clientes c ON p.cliente_id = c.id
        JOIN itens_pedido ip ON p.id = ip.pedido_id
        JOIN produtos pr ON ip.produto_id = pr.id
        WHERE p.id = $1
        GROUP BY p.id, c.nome, c.email, c.data_criacao, p.data_pedido, p.total, p.tempo_processamento
    `
	var id, clienteID int
	var clienteNome, clienteEmail string
	var clienteDataCriacao, dataPedido time.Time
	var total float64
	var tempoProcessamento time.Duration
	var itensJSON []byte
	err := e.Transacional.QueryRow(query, pedidoID).Scan(&id, &clienteID, &clienteNome, &clienteEmail, &clienteDataCriacao, &dataPedido, &total, &tempoProcessamento, &itensJSON)
	if err != nil {
		return fmt.Errorf("erro ao extrair dados do pedido: %v", err)
	}

	queryDW := `
        INSERT INTO pedidos_faturados (pedido_id, cliente_nome, cliente_email, cliente_data_criacao, data_pedido, total, tempo_processamento, itens)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `
	_, err = e.DW.Exec(queryDW, id, clienteNome, clienteEmail, clienteDataCriacao, dataPedido, total, tempoProcessamento, itensJSON)
	if err != nil {
		return fmt.Errorf("erro ao inserir no DW: %v", err)
	}

	return nil
}

// AdicionarFerramenta adds a new tool to the transactional database and triggers ETL
func (e *ETL) AdicionarFerramenta(ferramenta Ferramenta) (int, error) {
	query := `
        INSERT INTO produtos (nome, descricao, preco, estoque, categoria, material, marca, dimensoes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id
    `
	var id int
	err := e.Transacional.QueryRow(query,
		ferramenta.Nome, ferramenta.Descricao, ferramenta.Preco, ferramenta.Estoque,
		ferramenta.Categoria, ferramenta.Material, ferramenta.Marca, ferramenta.Dimensoes).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("erro ao adicionar ferramenta: %v", err)
	}

	if err := e.executarETLFerramenta(id); err != nil {
		log.Printf("Erro no ETL de ferramenta: %v", err)
	}

	return id, nil
}

// executarETLFerramenta consolidates tool data in the DW
func (e *ETL) executarETLFerramenta(produtoID int) error {
	query := `
        SELECT id, nome, descricao, preco, estoque, categoria, material, marca, dimensoes
        FROM produtos
        WHERE id = $1
    `
	var ferramenta Ferramenta
	err := e.Transacional.QueryRow(query, produtoID).Scan(
		&ferramenta.ID, &ferramenta.Nome, &ferramenta.Descricao, &ferramenta.Preco,
		&ferramenta.Estoque, &ferramenta.Categoria, &ferramenta.Material, &ferramenta.Marca,
		&ferramenta.Dimensoes)
	if err != nil {
		return fmt.Errorf("erro ao extrair dados da ferramenta: %v", err)
	}

	queryDW := `
        INSERT INTO ferramentas_adicionadas (produto_id, nome, descricao, preco, estoque, categoria, material, marca, dimensoes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `
	_, err = e.DW.Exec(queryDW,
		ferramenta.ID, ferramenta.Nome, ferramenta.Descricao, ferramenta.Preco,
		ferramenta.Estoque, ferramenta.Categoria, ferramenta.Material, ferramenta.Marca,
		ferramenta.Dimensoes)
	if err != nil {
		return fmt.Errorf("erro ao inserir no DW: %v", err)
	}

	return nil
}

// AtualizarFerramenta updates an existing tool and triggers ETL
func (e *ETL) AtualizarFerramenta(ferramenta Ferramenta) error {
	query := `
        UPDATE produtos
        SET nome = $1, descricao = $2, preco = $3, estoque = $4, categoria = $5, material = $6, marca = $7, dimensoes = $8
        WHERE id = $9
    `
	result, err := e.Transacional.Exec(query,
		ferramenta.Nome, ferramenta.Descricao, ferramenta.Preco, ferramenta.Estoque,
		ferramenta.Categoria, ferramenta.Material, ferramenta.Marca, ferramenta.Dimensoes, ferramenta.ID)
	if err != nil {
		return fmt.Errorf("erro ao atualizar ferramenta: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("erro ao verificar linhas afetadas: %v", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("nenhuma ferramenta encontrada com ID %d", ferramenta.ID)
	}

	if err := e.executarETLFerramentaAtualizada(ferramenta.ID); err != nil {
		log.Printf("Erro no ETL de ferramenta atualizada: %v", err)
	}

	return nil
}

// executarETLFerramentaAtualizada consolidates updated tool data in the DW
func (e *ETL) executarETLFerramentaAtualizada(produtoID int) error {
	query := `
        SELECT id, nome, descricao, preco, estoque, categoria, material, marca, dimensoes
        FROM produtos
        WHERE id = $1
    `
	var ferramenta Ferramenta
	err := e.Transacional.QueryRow(query, produtoID).Scan(
		&ferramenta.ID, &ferramenta.Nome, &ferramenta.Descricao, &ferramenta.Preco,
		&ferramenta.Estoque, &ferramenta.Categoria, &ferramenta.Material, &ferramenta.Marca,
		&ferramenta.Dimensoes)
	if err != nil {
		return fmt.Errorf("erro ao extrair dados da ferramenta: %v", err)
	}

	queryDW := `
        INSERT INTO ferramentas_atualizadas (produto_id, nome, descricao, preco, estoque, categoria, material, marca, dimensoes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `
	_, err = e.DW.Exec(queryDW,
		ferramenta.ID, ferramenta.Nome, ferramenta.Descricao, ferramenta.Preco,
		ferramenta.Estoque, ferramenta.Categoria, ferramenta.Material, ferramenta.Marca,
		ferramenta.Dimensoes)
	if err != nil {
		return fmt.Errorf("erro ao inserir no DW: %v", err)
	}

	return nil
}

// ListarFerramentas returns all tools from the transactional database
func (e *ETL) ListarFerramentas() ([]Ferramenta, error) {
	query := `
        SELECT id, nome, descricao, preco, estoque, categoria, material, marca, dimensoes
        FROM produtos
    `
	rows, err := e.Transacional.Query(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao listar ferramentas: %v", err)
	}
	defer rows.Close()

	var ferramentas []Ferramenta
	for rows.Next() {
		var f Ferramenta
		err := rows.Scan(&f.ID, &f.Nome, &f.Descricao, &f.Preco, &f.Estoque, &f.Categoria, &f.Material, &f.Marca, &f.Dimensoes)
		if err != nil {
			return nil, fmt.Errorf("erro ao escanear ferramenta: %v", err)
		}
		ferramentas = append(ferramentas, f)
	}

	return ferramentas, nil
}
