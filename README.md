# Projeto ETL
## Bruno Macedo Lemos -> 24005988
## Lucas Brites -> 24009893
## Murilo -> 24012855
## João Mucci -> 24008202
Esta é uma aplicação em Go que implementa um processo ETL (Extract, Transform, Load) para uma plataforma de e-commerce focada em ferramentas industriais. Ela gerencia clientes, ferramentas e pedidos, armazenando dados transacionais em um banco de dados PostgreSQL (ecommerce_ferramentas) e consolidando os dados em um data warehouse (dw_ferramentas). A aplicação utiliza o roteador Gorilla Mux e o driver PostgreSQL.
Recursos

# Rotas existentes

### Criação e autenticação dos clientes (/criar-conta, /login)
### Gerenciar produtos da ferramentaria (/adicionar-ferramenta, /listar-ferramentas, /atualizar-ferramenta)
### Venda do produto (/faturar-pedido)
### Processos ETL para sincronizar dados com o data warehouse

Estrutura do projeto
```
etl_project/
├── main.go
├── database/
├── └── database.go
├── handlers/
│   └── handlers.go
├── etl/
│   └── etl.go
├── go.mod
├── go.sum
```

# Passo a Passo para Testar a Aplicação
Siga estas instruções para configurar e testar a aplicação localmente.
1. Clonar o Repositório

2. Instalar Dependências
Certifique-se de ter o Go instalado. Inicialize o módulo e instale as dependências:
go mod init etl_project
go mod tidy
go get github.com/gorilla/mux github.com/lib/pq

3. Configurar o Banco de Dados PostgreSQL
A aplicação usa dois bancos PostgreSQL: ecommerce_ferramentas (transacional) e dw_ferramentas (data warehouse).
a. Criar os Bancos
Abra o terminal ou pgAdmin4 e crie os bancos:

CREATE DATABASE ecommerce_ferramentas;
CREATE DATABASE dw_ferramentas;

b. Aplicar os Schemas
Execute os scripts SQL para criar as tabelas:

Para ecommerce_ferramentas:psql -U postgres -d ecommerce_ferramentas -f schema_ecommerce.sql

Para dw_ferramentas:psql -U postgres -d dw_ferramentas -f schema_dw.sql

Crie as tabelas
para o Banco -> ecommerce_ferramentas
```
CREATE TABLE clientes (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    senha VARCHAR(255) NOT NULL,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: produtos
-- Stores tool/product information
CREATE TABLE produtos (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    preco DECIMAL(10, 2) NOT NULL,
    estoque INTEGER NOT NULL,
    categoria VARCHAR(50) NOT NULL,
    material VARCHAR(50),
    marca VARCHAR(50),
    dimensoes VARCHAR(50)
);

-- Table: pedidos
-- Stores order information
CREATE TABLE pedidos (
    id SERIAL PRIMARY KEY,
    cliente_id INTEGER REFERENCES clientes(id),
    data_pedido TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    tempo_processamento INTERVAL
);

-- Table: itens_pedido
-- Stores order items, linking orders to products
CREATE TABLE itens_pedido (
    id SERIAL PRIMARY KEY,
    pedido_id INTEGER REFERENCES pedidos(id),
    produto_id INTEGER REFERENCES produtos(id),
    quantidade INTEGER NOT NULL,
    preco_unitario DECIMAL(10, 2) NOT NULL
);
```

para o banco -> dw_ferramentas

```
CREATE TABLE pedidos_faturados (
    id SERIAL PRIMARY KEY,
    pedido_id INTEGER NOT NULL,
    cliente_nome VARCHAR(100) NOT NULL,
    cliente_email VARCHAR(100) NOT NULL,
    cliente_data_criacao TIMESTAMP NOT NULL,
    data_pedido TIMESTAMP NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    tempo_processamento INTERVAL,
    itens JSONB NOT NULL,
    data_faturamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: ferramentas_adicionadas
-- Stores data for newly added tools
CREATE TABLE ferramentas_adicionadas (
    id SERIAL PRIMARY KEY,
    produto_id INTEGER NOT NULL,
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    preco DECIMAL(10, 2) NOT NULL,
    estoque INTEGER NOT NULL,
    categoria VARCHAR(50) NOT NULL,
    material VARCHAR(50),
    marca VARCHAR(50),
    dimensoes VARCHAR(50),
    data_adicao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: ferramentas_atualizadas
-- Stores data for updated tools
CREATE TABLE ferramentas_atualizadas (
    id SERIAL PRIMARY KEY,
    produto_id INTEGER NOT NULL,
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    preco DECIMAL(10, 2) NOT NULL,
    estoque INTEGER NOT NULL,
    categoria VARCHAR(50) NOT NULL,
    material VARCHAR(50),
    marca VARCHAR(50),
    dimensoes VARCHAR(50),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```


c. Verificar Credenciais
A aplicação usa user=postgres e password=postgres (hardcoded em handlers/handlers.go). Se o seus dados setados no PostGres forem diferentes, por favor, mude o arquivo handlers.go ou os seus dados:
ALTER USER postgres PASSWORD 'postgres';

4. Iniciar o Servidor
Execute a aplicação no terminal:
go run main.go

Você verá:
Servidor rodando na porta 8080...

5. Testar as Rotas da API
### Criação e autenticação dos clientes (/criar-conta, /login)
## POST
curl.exe -X POST http://localhost:8080/criar-conta -H "Content-Type: application/json" -d '{"nome":"Bruno Macedo","email":"bruno@exemplo.com","senha":"123456"}'
curl.exe -X POST http://localhost:8080/login -H "Content-Type: application/json" -d '{"email":"bruno@exemplo.com","senha":"123456"}'

### Gerenciar produtos da ferramentaria (/adicionar-ferramenta, /listar-ferramentas, /atualizar-ferramenta)
## POST
curl.exe -X POST http://localhost:8080/adicionar-ferramenta -H "Content-Type: application/json" -d '{"nome":"Martelo Stanley","descricao":"Martelo de garra 500g","preco":49.90,"estoque":100,"categoria":"Ferramentas Manuais","material":"Aco","marca":"Stanley","dimensoes":"30x5x3 cm"}'
## GET
curl.exe -X GET http://localhost:8080/listar-ferramentas
## PUT
curl.exe -X PUT http://localhost:8080/atualizar-ferramenta -H "Content-Type: application/json" -d '{"id":1,"nome":"Martelo Stanley","descricao":"Martelo de garra 500g atualizado","preco":59.90,"estoque":80,"categoria":"Ferramentas Manuais","material":"Aco","marca":"Stanley","dimensoes":"30x5x3 cm"}'


### Venda do produto (/faturar-pedido)
## POST
curl.exe -X POST http://localhost:8080/faturar-pedido -H "Content-Type: application/json" -d '{"cliente_id":1,"itens":[{"produto_id":1,"quantidade":2,"preco":59.90}]}'

6. Verificar os Dados no Banco
Use o pgAdmin 4 ou psql para verificar os dados após os testes.
a. Banco Transacional (ecommerce_ferramentas)
\c ecommerce_ferramentas
SELECT * FROM clientes;        -- Cliente criado
SELECT * FROM produtos;        -- Ferramenta adicionada/atualizada
SELECT * FROM pedidos;         -- Pedido faturado
SELECT * FROM itens_pedido;    -- Itens do pedido

b. Data Warehouse (dw_ferramentas)
\c dw_ferramentas
SELECT * FROM pedidos_faturados;       -- Pedido consolidado
SELECT * FROM ferramentas_adicionadas; -- Ferramenta adicionada
SELECT * FROM ferramentas_atualizadas; -- Ferramenta atualizada
