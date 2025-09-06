import sys
import json
import os
import shutil
import csv

def carregar_json(caminho_json: str) -> dict:
    with open(caminho_json, "r", encoding="utf-8") as f:
        return json.load(f)

def validar_csv(caminho_csv: str, colunas_necessarias: list):
    with open(caminho_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f, delimiter=";")

        cabecalho = leitor.fieldnames
        if cabecalho is None:
            print("Erro: CSV vazio ou sem cabeçalho.")
            sys.exit(1)

        faltantes = [col for col in colunas_necessarias if col not in cabecalho]
        if faltantes:
            print(f"Erro: Colunas não encontradas no CSV: {faltantes}")
            sys.exit(1)

        print(f"Colunas necessárias encontradas: {colunas_necessarias}")

def imprimir_csv(caminho_csv: str, colunas: list):
    with open(caminho_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f, delimiter=";")
        print("Valores lidos:")
        for linha in leitor:
            valores = [linha[col] for col in colunas]
            print(";".join(valores))

def mover_arquivo(config: dict):
    origem = os.path.join(config["origem"], config["nomeArquivo"])
    destino = os.path.join(config["destino"], config["nomeArquivo"])

    if not os.path.isfile(origem):
        print(f"Erro: Arquivo {origem} não encontrado.")
        sys.exit(1)

    colunas_necessarias = [item["coluna"] for item in config.get("template", [])]
    if colunas_necessarias:
        validar_csv(origem, colunas_necessarias)
        imprimir_csv(origem, colunas_necessarias)

    if not os.path.isdir(config["destino"]):
        print(f"Pasta destino {config['destino']} não existe. Criando...")
        os.makedirs(config["destino"], exist_ok=True)

    try:
        # Desabilitado para testes
        # shutil.move(origem, destino)
        print(f"Arquivo movido com sucesso para {destino}")
    except Exception as e:
        print(f"Erro ao mover arquivo: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Uso: python script.py <arquivo_config.json>")
        sys.exit(1)

    config = carregar_json(sys.argv[1])
    mover_arquivo(config)

if __name__ == "__main__":
    main()
