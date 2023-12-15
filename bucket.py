import json
import sys
import os
import socket
from messages import BucketSubscribeMessage


class Bucket:
    def __init__(self, bucket_id: str):
        self.bucket_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bucket_socket.connect(("localhost", 12345))
        self.bucket_id = bucket_id
        print(f"[{self.bucket_id}] Conectado ao gateway.")
        self.connect_with_gateway()

    def connect_with_gateway(self):
        while True:
            self.bucket_socket.send(
                BucketSubscribeMessage(bucket_id=self.bucket_id, address=self.bucket_socket.getsockname()).encode()
            )
            response = json.loads(self.bucket_socket.recv(1024).decode("utf-8"))
            print(f"Recebido: {response}")
            match response:
                case {
                    "type": "store_file",
                    "user_id": user_id,
                    "file_name": file_name,
                    "file_content": file_content,
                    "number_of_replicas": _number_of_replicas,
                }:
                    self.store_file(user_id, file_name, file_content.encode("utf-8"))
                case {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}:
                    self.retrieve_file(user_id, file_name)
                case _:
                    print("Mensagem inválida")

    def store_file(self, user_id: str, file_name: str, file_content: bytes):
        directory_path = f"store/{self.bucket_id}/{user_id}"
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, file_name)
        with open(file_path, "wb") as file:
            file.write(file_content)
        print(f"User: {user_id} Armazenou arquivo {file_name} com sucesso.")

    def retrieve_file(self, user_id: str, file_name: str) -> bytes:
        file_path = f"store/{self.bucket_id}/{user_id}/{file_name}"
        if os.path.exists(file_path):
            with open(file_path, "rb") as file:
                print(f"Recuperando arquivo {file_name} para o usuário {user_id}...")
                return file.read()
        else:
            print(f"Arquivo {file_name} não encontrado.")
            return b""

    def list_users(self):
        current_location = os.getcwd()
        bucket_path = os.path.join(current_location, "store", self.bucket_id)
        try:
            # Listar todos os subdiretórios no diretório do bucket
            return [name for name in os.listdir(bucket_path) if os.path.isdir(os.path.join(bucket_path, name))]
        except FileNotFoundError:
            print(f"Nenhum dado encontrado para o bucket {self.bucket_id}")
            return []


if __name__ == "__main__":
    Bucket(sys.argv[1])
