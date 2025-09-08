import sys
import json
import os
import shutil
import csv
import pyodbc
from dotenv import load_dotenv

# Carregamento de Variáveis de ambiente .env
load_dotenv()
CSV_DELIMITER = os.getenv("CSV_DELIMITER", ";")
MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_USER = os.getenv("MSSQL_USER")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")


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
        leitor = csv.DictReader(f, delimiter=CSV_DELIMITER)

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
        leitor = csv.DictReader(f, delimiter=CSV_DELIMITER)
        print("Valores lidos:")
        for linha in leitor:
            valores = [linha[col] for col in colunas]
            print(";".join(valores))

def inserir_csv_sqlserver(caminho_csv: str, config: dict):
    """
    Conecta ao SQL Server, lê o CSV e insere os dados no banco de dados.
    As credenciais são carregadas de um arquivo .env.
    """    
    
    conexao_str = f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_SERVER};DATABASE={config['database']};UID={MSSQL_USER};PWD={MSSQL_PASSWORD}"
    conexao = None
    try:
        conexao = pyodbc.connect(conexao_str)
        cursor = conexao.cursor()

        colunas_origem = [item["colunaFonte"] for item in config["template"]]
        colunas_destino = [item["colunaRemoto"] for item in config["template"]]

        with open(caminho_csv, "r", encoding="utf-8") as f:
            leitor = csv.DictReader(f, delimiter=CSV_DELIMITER)
            
            # Prepara a instrução SQL de inserção
            placeholders = ",".join("?" for _ in colunas_origem)
            sql = f"INSERT INTO {config['database']}.dbo.{config['tabela']} ({','.join(colunas_destino)}) VALUES ({placeholders})"
            
            # Cria uma lista para armazenar os dados do lote
            dados_lote = []
            lote_tamanho = 1000  # Tamanho do lote, pode ser ajustado conforme a necessidade
            
            for linha in leitor:
                valores = [linha[col] for col in colunas_origem]
                dados_lote.append(valores)
                
                # Quando o lote atinge o tamanho definido, executa a inserção em massa
                if len(dados_lote) >= lote_tamanho:
                    cursor.executemany(sql, dados_lote)
                    dados_lote = []  # Limpa a lista para o próximo lote

            # Insere os registros restantes, se houver
            if dados_lote:
                cursor.executemany(sql, dados_lote)

        conexao.commit()
        print("Todos os registros foram inseridos com sucesso no SQL Server.")

    except pyodbc.Error as e:
        if conexao:
            conexao.rollback()
        registra_mensagem(f"Erro ao inserir dados. A transação foi revertida: {e}")
        sys.exit(1)
    except Exception as e:
        if conexao:
            conexao.rollback()
        registra_mensagem(f"Erro inesperado: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conexao:
            conexao.close()

def executar_procedure(config: dict):
    """
    Função para executar uma procedure dentro do SQL SERVER
    """
    key = "procedure"
    
    if key not in config:
        # print("procedure não localizada")
        # Caso não haja procedure para executar, encerra procedure
        return
    
    conexao_str = f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_SERVER};DATABASE={config['database']};UID={MSSQL_USER};PWD={MSSQL_PASSWORD}"
    
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
