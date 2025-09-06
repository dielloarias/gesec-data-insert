import sys
import json
import os
import shutil

def carregar_json(caminho_json: str) -> dict:
    with open(caminho_json, "r", encoding="utf-8") as f:
        return json.load(f)

def mover_arquivo(config: dict):
    origem = os.path.join(config["origem"], config["nomeArquivo"])
    destino = os.path.join(config["destino"], config["nomeArquivo"])

    if not os.path.isfile(origem):
        print(f"Erro: Arquivo {origem} não encontrado.")
        sys.exit(1)

    if not os.path.isdir(config["destino"]):
        print(f"Pasta destino {config['destino']} não existe. Criando...")
        os.makedirs(config["destino"], exist_ok=True)

    try:
        shutil.move(origem, destino)
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
