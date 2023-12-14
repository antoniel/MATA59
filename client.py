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


def main():
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cliente.connect(("localhost", 12345))

    while True:
        modo = input("Escolha o modo (deposito/recuperacao/sair): ")
        nome_arquivo = input("Digite o nome do arquivo: ")

        if modo == "deposito":
            with open(nome_arquivo, "rb") as arquivo:
                conteudo = arquivo.read()
            dados = {
                "modo": "deposito",
                "nome_arquivo": nome_arquivo,
                "conteudo": conteudo,
            }
            enviar_json(cliente, dados)

        elif modo == "recuperacao":
            dados = {"modo": "recuperacao", "nome_arquivo": nome_arquivo}
            enviar_json(cliente, dados)

        elif modo == "sair":
            break

    cliente.close()


if __name__ == "__main__":
    main()
