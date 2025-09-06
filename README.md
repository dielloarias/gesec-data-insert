# Projeto ETL CSV -> SQL Server

Autor: **Diello Cardoso de La Paz Arias**

---

## 📌 Descrição

Este projeto em Python realiza um processo simples de ETL (Extract, Transform, Load) para dados em arquivos CSV.O fluxo consiste em:

1. Ler um arquivo de configuração JSON que contém:

   - Nome do arquivo CSV.
   - Pasta de origem.
   - Pasta de destino.
   - Banco de dados e tabela de destino no SQL Server.
   - Template de colunas (mapeamento entre colunas do CSV e colunas da tabela).
2. Validar se as colunas necessárias existem no CSV.
3. Imprimir no console os valores das colunas especificadas.
4. Inserir os registros no banco de dados SQL Server conforme o mapeamento.
5. Mover o arquivo da pasta de origem para a pasta de destino.

---

## 📌 Estrutura do `config.json`

```json
{
    "nomeArquivo": "teste.csv",
    "origem": "./pasta-origem",
    "destino": "./pasta-destino",
    "database": "DB_PAISES",
    "tabela": "PAISES",
    "template": [
        { "colunaFonte": "id", "colunaRemoto": "codigo" },
        { "colunaFonte": "pais", "colunaRemoto": "nm_pais" },
        { "colunaFonte": "capital", "colunaRemoto": "nm_capital" }
    ]
}
```

---

### 📌 Arquivo `.env`

As credenciais do SQL Server devem ser armazenadas em um arquivo `.env` na raiz do projeto:

```env
MSSQL_SERVER=localhost
MSSQL_USER=seu_usuario
MSSQL_PASSWORD=sua_senha
MSSQL_DRIVER=ODBC Driver 17 for SQL Server
```

---

## 📦 Requisitos

Instale os pacotes necessários com:

```bash
pip install pyodbc python-dotenv
```

⚠️ Além disso, é necessário ter o **ODBC Driver 17 for SQL Server** instalado no sistema.

---

## ▶️ Execução

Para rodar o script, use o seguinte comando:

```bash
python script.py config.json
```

---

## 🔄 Fluxo do Script

1. Leitura do `config.json`.
2. Validação do CSV.
3. Impressão dos valores das colunas.
4. Inserção dos registros no SQL Server.
5. Movimentação do arquivo para a pasta destino.

---

## ⚙️Como executar o script

1. Certifique-se de ter instalado os pacotes necessários:

```bash
pip install pyodbc python-dotenv
```

2. Configure o arquivo `.env` com as credenciais do `SQL Server`.
3. Prepare o arquivo `config.json` com a estrutura esperada.
4. Execute o script passando o caminho do arquivo de configuração como parâmetro:

```bash
python script.py config.json
```
