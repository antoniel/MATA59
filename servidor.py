import socket
from util_json import receber_json


def armazenar_arquivo(conexao, nome_arquivo, dados):
    print(f"Recebendo arquivo {nome_arquivo}...")
    print(f"Tamanho do arquivo: {len(dados)} bytes")
    conexao.sendall("OK".encode("utf-8"))


def recuperar_arquivo(conexao, nome_arquivo):
    print(f"Recuperando arquivo {nome_arquivo}...")
    conexao.sendall("OK".encode("utf-8"))


def main():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind(("localhost", 12345))
    servidor.listen(1)
    print("Servidor pronto para conexões.")

    while True:
        conexao, endereco = servidor.accept()
        print(f"Conexão estabelecida com {endereco}")

        dados = receber_json(conexao)
        modo = dados["modo"]

        if modo == "deposito":
            armazenar_arquivo(conexao, dados["nome_arquivo"], dados["conteudo"])

        elif modo == "recuperacao":
            recuperar_arquivo(conexao, dados["nome_arquivo"])

        conexao.close()


if __name__ == "__main__":
    main()
