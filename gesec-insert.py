import sys
import json
import os
import shutil
import pyodbc
from dotenv import load_dotenv

# Carregamento de Variáveis de ambiente .env
load_dotenv()
MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_USER = os.getenv("MSSQL_USER")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")


def registra_mensagem(mensagem: str):
    """Função para registrar mensagens no console."""
    print(mensagem)


def carregar_json(caminho_json: str) -> dict:
    """Carrega um arquivo JSON e retorna seu conteúdo como dicionário."""
    with open(caminho_json, "r", encoding="utf-8") as f:
        return json.load(f)


def validar_json(caminho_json: str, colunas_necessarias: list):
    """Valida se os registros JSON possuem as colunas necessárias."""
    dados = carregar_json(caminho_json)

    if "records" not in dados or not isinstance(dados["records"], list):
        registra_mensagem("Erro: JSON inválido. Esperado campo 'records' como lista.")
        sys.exit(1)

    if not dados["records"]:
        registra_mensagem("Erro: Nenhum registro encontrado no JSON.")
        sys.exit(1)

    faltantes = []
    for col in colunas_necessarias:
        if any(col not in registro for registro in dados["records"]):
            faltantes.append(col)

    if faltantes:
        registra_mensagem(f"Erro: Colunas não encontradas em alguns registros: {faltantes}")
        sys.exit(1)

    print(f"Colunas necessárias encontradas: {colunas_necessarias}")
    return dados["records"]


def inserir_json_sqlserver(caminho_json: str, config: dict):
    """Conecta ao SQL Server, lê o JSON e insere os dados no banco de dados."""
    conexao_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_SERVER};"
        f"DATABASE={config['database']};UID={MSSQL_USER};PWD={MSSQL_PASSWORD}"
    )
    conexao = None
    try:
        conexao = pyodbc.connect(conexao_str)
        cursor = conexao.cursor()

        colunas_origem = [item["colunaFonte"] for item in config["template"]]
        colunas_destino = [item["colunaRemoto"] for item in config["template"]]

        registros = validar_json(caminho_json, colunas_origem)

        # Prepara a instrução SQL de inserção
        placeholders = ",".join("?" for _ in colunas_origem)
        colunas_com_colchetes = [f"[{coluna}]" for coluna in colunas_destino]
        colunas_string = ", ".join(colunas_com_colchetes)

        sql = (
            f"INSERT INTO {config['database']}.dbo.{config['tabela']} "
            f"({colunas_string}) VALUES ({placeholders});"
        )

        print(sql)

        dados_lote = []
        lote_tamanho = 1000

        for registro in registros:
            valores = [registro[col] for col in colunas_origem]
            dados_lote.append(valores)

            if len(dados_lote) >= lote_tamanho:
                cursor.executemany(sql, dados_lote)
                dados_lote = []

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
    """Função para executar uma procedure dentro do SQL SERVER."""
    key = "procedure"

    if key not in config:
        return

    conexao_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_SERVER};"
        f"DATABASE={config['database']};UID={MSSQL_USER};PWD={MSSQL_PASSWORD}"
    )

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
    Valida o JSON, insere no SQL Server e move o arquivo.
    """
    origem = os.path.join(config["origem"], config["nomeArquivo"])
    destino = os.path.join(config["destino"], config["nomeArquivo"])

    if not os.path.isfile(origem):
        registra_mensagem(f"Erro: Arquivo {origem} não encontrado.")
        sys.exit(1)

    colunas_necessarias = [item["colunaFonte"] for item in config.get("template", [])]
    if colunas_necessarias:
        inserir_json_sqlserver(origem, config)
        executar_procedure(config)

    if not os.path.isdir(config["destino"]):
        print(f"Pasta destino {config['destino']} não existe. Criando...")
        os.makedirs(config["destino"], exist_ok=True)

    try:
        # shutil.move(origem, destino)
        print(f"Arquivo movido com sucesso para {destino}")
    except Exception as e:
        registra_mensagem(f"Erro ao mover arquivo: {e}")
        sys.exit(1)


def main():
    """Ponto de entrada do script."""
    if len(sys.argv) != 2:
        print("Uso: python script.py <arquivo_config.json>")
        sys.exit(1)

    config = carregar_json(sys.argv[1])
    mover_arquivo(config)


if __name__ == "__main__":
    main()
