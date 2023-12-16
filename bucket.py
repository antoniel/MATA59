import json
import os
import socket
from utils import print_info, print_receive, send_print, CommunicationManager


class Bucket(CommunicationManager):
    def __init__(self, bucket_id: str):
        # super serviceName
        super().__init__("Bucket 📦")
        self.bucket_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bucket_socket.connect(("localhost", 12345))
        self.bucket_id = bucket_id
        print(f"[{self.bucket_id}] Conectado ao gateway.")
        self.connect_with_gateway()

    def connect_with_gateway(self):
        while True:
            self.send_message(self.bucket_socket, {"type": "subscribe_bucket", "bucket_id": self.bucket_id}, "gateway")
            response = self.bucket_socket.recv(1024).decode("utf-8")
            print_receive(f"Recebido: {response}")

            if not response:
                break

            response = json.loads(response)
            match response:
                case {"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content}:
                    self.store_file(user_id, file_name, file_content.encode("utf-8"))
                case {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}:
                    print_info(f"Enviando arquivo {file_name} para o usuário {user_id}...")
                    file_content = self.retrieve_file(user_id, file_name)

                    data = {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}
                    send_print(f"Enviando: {data}")
                    json_response = json.dumps(data)
                    self.bucket_socket.send(json_response.encode("utf-8"))
                case {"type": "OK"}:
                    print_receive(response)
                case _:
                    print("Mensagem inválida")

    def store_file(self, user_id: str, file_name: str, file_content: bytes):
        directory_path = f"store/{self.bucket_id}/{user_id}"
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, file_name)
        with open(file_path, "wb") as file:
            file.write(file_content)
        print_info(f"User: {user_id} Armazenou arquivo {file_name} com sucesso.")

    def retrieve_file(self, user_id: str, file_name: str) -> bytes:
        file_path = f"store/{self.bucket_id}/{user_id}/{file_name}"
        if os.path.exists(file_path):
            with open(file_path, "rb") as file:
                print_info(f"Recuperando arquivo {file_name} para o usuário {user_id}...")
                return file.read().decode("utf-8")
        else:
            print_info(f"Arquivo {file_name} não encontrado.")
            return b""

    def list_users(self):
        current_location = os.getcwd()
        bucket_path = os.path.join(current_location, "store", self.bucket_id)
        try:
            # Listar todos os subdiretórios no diretório do bucket
            return [name for name in os.listdir(bucket_path) if os.path.isdir(os.path.join(bucket_path, name))]
        except FileNotFoundError:
            print_info(f"Nenhum dado encontrado para o bucket {self.bucket_id}")
            return []


if __name__ == "__main__":
    # Bucket(sys.argv[1])
    Bucket("s3")
