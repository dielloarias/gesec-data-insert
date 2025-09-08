import sys
import json
import os
import shutil
import csv
import pyodbc
from dotenv import load_dotenv

def registra_mensagem(mensagem: str):
    """
    Função para registrar mensagens no console.
    Pode ser facilmente modificada no futuro para escrever em um arquivo de log.
    """
    print(mensagem)

def carregar_json(caminho_json: str) -> dict:
    """
    Carrega um arquivo JSON e retorna seu conteúdo como um dicionário.
    """    
    with open(caminho_json, "r", encoding="utf-8") as f:
        return json.load(f)

def validar_csv(caminho_csv: str, colunas_necessarias: list):
    """
    Valida se um arquivo CSV possui as colunas necessárias.
    Encerra o script com um erro se o CSV for inválido.
    """    
    with open(caminho_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f, delimiter=";")

        cabecalho = leitor.fieldnames
        if cabecalho is None:
            registra_mensagem("Erro: CSV vazio ou sem cabeçalho.")
            sys.exit(1)

        faltantes = [col for col in colunas_necessarias if col not in cabecalho]
        if faltantes:
            registra_mensagem(f"Erro: Colunas não encontradas no CSV: {faltantes}")
            sys.exit(1)

        print(f"Colunas necessárias encontradas: {colunas_necessarias}")

def imprimir_csv(caminho_csv: str, colunas: list):
    """
    Lê o CSV e imprime os valores das colunas especificadas.
    Útil para visualização e depuração.
    """    
    with open(caminho_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f, delimiter=";")
        print("Valores lidos:")
        for linha in leitor:
            valores = [linha[col] for col in colunas]
            print(";".join(valores))

def inserir_csv_sqlserver(caminho_csv: str, config: dict):
    """
    Conecta ao SQL Server, lê o CSV e insere os dados no banco de dados.
    As credenciais são carregadas de um arquivo .env.
    """    
    load_dotenv()  # carrega variáveis do .env

    server = os.getenv("MSSQL_SERVER")
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

    conexao_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={config['database']};UID={user};PWD={password}"
    try:
        conexao = pyodbc.connect(conexao_str)
        cursor = conexao.cursor()
    except Exception as e:
        registra_mensagem(f"Erro ao conectar ao SQL Server: {e}")
        sys.exit(1)

    colunas_origem = [item["colunaFonte"] for item in config["template"]]
    colunas_destino = [item["colunaRemoto"] for item in config["template"]]

    with open(caminho_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f, delimiter=";")
        for linha in leitor:
            valores = [linha[col] for col in colunas_origem]
            placeholders = ",".join("?" for _ in valores)
            sql = f"INSERT INTO {config['database']}.dbo.{config['tabela']} ({','.join(colunas_destino)}) VALUES ({placeholders})"
            try:
                cursor.execute(sql, valores)
            except Exception as e:
                registra_mensagem(f"Erro ao inserir registro {valores}: {e}")
                conexao.rollback()
                cursor.close()
                conexao.close()
                sys.exit(1)

    conexao.commit()
    cursor.close()
    conexao.close()
    print("Todos os registros foram inseridos com sucesso no SQL Server.")

def executar_procedure(config: dict):
    """
    Função para executar uma procedure dentro do SQL SERVER
    """
    key = "procedure"
    
    if key not in config:
        # print("procedure não localizada")
        # Caso não haja procedure para executar, encerra procedure
        return
    
    load_dotenv()  # carrega variáveis do .env

    server = os.getenv("MSSQL_SERVER")
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

    conexao_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={config['database']};UID={user};PWD={password}"
    
    try:
        procedure = config[key]
        database = config["database"]
        
        conexao = pyodbc.connect(conexao_str)
        cursor = conexao.cursor()
        cursor.execute(f"EXEC [{database}].[dbo].[{procedure}];")
        conexao.commit()
        cursor.close()
        conexao.close()
        
        print(f"Procedure [{database}].[dbo].[{procedure}] executada com sucesso.")
        
    except Exception as e:
        registra_mensagem(f"Erro ao executar a procedure: {e}")
        sys.exit(1)

def mover_arquivo(config: dict):
    """
    Função principal que orquestra todo o processo de ETL.
    Valida o CSV, insere no SQL Server e move o arquivo.
    """
    origem = os.path.join(config["origem"], config["nomeArquivo"])
    destino = os.path.join(config["destino"], config["nomeArquivo"])

    if not os.path.isfile(origem):
        registra_mensagem(f"Erro: Arquivo {origem} não encontrado.")
        sys.exit(1)

    colunas_necessarias = [item["colunaFonte"] for item in config.get("template", [])]
    if colunas_necessarias:
        validar_csv(origem, colunas_necessarias)
        # imprimir_csv(origem, colunas_necessarias)
        inserir_csv_sqlserver(origem, config)
        executar_procedure(config)

    if not os.path.isdir(config["destino"]):
        print(f"Pasta destino {config['destino']} não existe. Criando...")
        os.makedirs(config["destino"], exist_ok=True)

    try:
        shutil.move(origem, destino)
        print(f"Arquivo movido com sucesso para {destino}")
    except Exception as e:
        registra_mensagem(f"Erro ao mover arquivo: {e}")
        sys.exit(1)

def main():
    """
    Ponto de entrada do script.
    Valida os argumentos de linha de comando e inicia o processo.
    """    
    if len(sys.argv) != 2:
        print("Uso: python script.py <arquivo_config.json>")
        sys.exit(1)

    config = carregar_json(sys.argv[1])
    mover_arquivo(config)

if __name__ == "__main__":
    main()
