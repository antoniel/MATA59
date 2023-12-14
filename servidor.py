import socket
from typing import Dict, Union, Literal
import json
import base64


def maybe_json_parse(message) -> Union[Dict, None]:
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        print("Não foi possível parsear a mensagem")
        return None


def receber_json(conexao, tamanho_buffer=1024):
    mensagem_codificada = conexao.recv(tamanho_buffer)
    mensagem_json = mensagem_codificada.decode("utf-8")
    dados = json.loads(mensagem_json)
    if "conteudo" in dados:
        dados["conteudo"] = base64.b64decode(dados["conteudo"].encode("utf-8"))
    return dados


def armazenar_arquivo(nome_arquivo, dados):
    print(f"Recebendo arquivo {nome_arquivo}...")
    print(f"Tamanho do arquivo: {len(dados)} bytes")


def recuperar_arquivo(nome_arquivo):
    print(f"Recuperando arquivo {nome_arquivo}...")


class Message:
    type: Literal["subscribe_bucket", "subscribe_client"]
    address: str

    def encode(self):
        return json.dumps(self.__dict__).encode("utf-8")


# echo '{"type":"subscribe_bucket", "bucket_id": "s1"}' | nc localhost 12345
class SubscribeAsBucketMessage(Message):
    bucket_id: str

    def __init__(self, bucket_id: str, address):
        self.type = "subscribe_bucket"
        self.address = address
        self.bucket_id = bucket_id


# echo '{"type":"subscribe_client", "user_id": "u1"}' | nc localhost 12345
class SubscribeAsClient(Message):
    user_id: str

    def __init__(self, user_id: str, address):
        self.type = "subscribe_client"
        self.address = address
        self.user_id = user_id


class Bucket:
    bucketId: str
    files: Dict[str, bytes]

    def __init__(self):
        self.files = {}  # Dicionário para armazenar os arquivos

    def store_file(self, user_id: str, file_name: str, file_content: bytes):
        print(f"User: {user_id} Armazenando arquivo {file_name}...")
        self.files[file_name] = file_content
        print(f"Arquivo {file_name} armazenado com sucesso.")

    def retrieve_file(self, file_name: str) -> bytes:
        print(f"Recuperando arquivo {file_name}...")
        file_content = self.files.get(file_name)
        if file_content is None:
            print(f"Arquivo {file_name} não encontrado.")
            return b""
        return file_content


class Main:
    connection: socket.socket
    buckets: Dict[str, SubscribeAsBucketMessage] = {}

    def __init__(self):
        appSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        appSocket.bind(("localhost", 12345))
        appSocket.listen(5)
        print("Servidor pronto para conexões.")

        while True:
            self.connection, address = appSocket.accept()
            data = maybe_json_parse(self.connection.recv(1024).decode("utf-8"))
            match data:
                case {"type": "subscribe_bucket"}:
                    bucket = SubscribeAsBucketMessage(address=address, bucket_id=data["bucket_id"])
                    print(f"{address} Subscribing as bucket | bucket_id: {bucket.bucket_id}")
                    self.buckets[bucket.bucket_id] = bucket
                    self.OK()
                case {"type": "subscribe_client"}:
                    client = SubscribeAsClient(address=address, user_id=data["user_id"])
                    print(f"{address} Subscribing as client | user_id: {client.user_id}")
                    self.OK()
                case _:
                    print("Invalid message")
                    self.ERROR()

            self.connection.close()

    def OK(self):
        self.connection.send("OK".encode("utf-8"))

    def ERROR(self):
        self.connection.send("ERROR".encode("utf-8"))


if __name__ == "__main__":
    Main()
