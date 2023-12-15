import sys
import base64
import json
import socket
from util_json import enviar_json


def enviar_modo(conexao, modo):
    conexao.sendall(modo.encode("utf-8"))


def depositar_arquivo(conexao, nome_arquivo):
    with open(nome_arquivo, "rb") as arquivo:
        dados = arquivo.read()
    tamanho_arquivo = str(len(dados))
    conexao.sendall(nome_arquivo.encode("utf-8"))
    conexao.sendall(tamanho_arquivo.encode("utf-8"))
    conexao.sendall(dados)


def recuperar_arquivo(conexao, nome_arquivo):
    conexao.sendall(nome_arquivo.encode("utf-8"))
    dados = conexao.recv(1024)
    with open(f"recuperado_{nome_arquivo}", "wb") as arquivo:
        arquivo.write(dados)


def main(user_id):
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.connect(("localhost", 12345))

    while True:
        # modo = input("Escolha o modo (deposito/recuperacao/sair): ")
        modo = "deposito"
        # number_of_replicas = input("Numero de replicas:")
        number_of_replicas = 1
        # file_name = input("Digite o nome do arquivo: ")
        file_name = "some.txt"

        if modo == "deposito":
            with open(file_name, "rb") as arquivo:
                file_content = arquivo.read()
            dados = json.dumps(
                {
                    "type": "store_file",
                    "user_id": user_id,
                    "file_name": file_name,
                    "file_content": file_content.decode("utf8"),
                    "number_of_replicas": number_of_replicas,
                }
            )
            print(dados)
            connection.send(dados.encode("utf-8"))
            break

        elif modo == "recuperacao":
            dados = {"modo": "recuperacao", "nome_arquivo": file_name}
            enviar_json(connection, dados)

        elif modo == "sair":
            break

    connection.close()


if __name__ == "__main__":
    # main(sys.argv[1])
    main("dib")
