import sys
import os
import socket
import threading
from utils import print_info, _print_receive, CommunicationManager


class Bucket(CommunicationManager):
    def __init__(self, bucket_id: str):
        # super serviceName
        super().__init__(f"Bucket [{bucket_id}]")
        bucket_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bucket_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bucket_socket.connect(("localhost", 12345))
        self.bucket_id = bucket_id

        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind(("localhost", 0))  # Bind to any available address and port
        self.listener_socket.listen()
        address, port = self.listener_socket.getsockname()

        threading.Thread(target=self.handle_incoming_connections).start()
        threading.Thread(target=self.connect_with_gateway, args=(bucket_socket, address, port)).start()

        # self.connect_with_gateway()

    def connect_with_gateway(self, connection, address=None, port=None):
        print_info(f"{str(threading.get_ident())[-2:]} | [{self.bucket_id}] Conectado ao gateway.")
        while True:
            self.send_message(connection, {"type": "subscribe_bucket", "bucket_id": self.bucket_id, "address": address, "port": port}, "Gateway")
            messages = self.receive_messages(connection)
            if len(messages) == 1 and not messages[0]:
                break
            for message in messages:
                if not message:
                    break
                self.match_message(connection, messages, message)

    def handle_incoming_connections(self):
        print_info(f"{str(threading.get_ident())[-2:]} | [{self.bucket_id}] Aguardando conexões...")
        while True:
            (connection, _) = self.listener_socket.accept()
            messages = self.receive_messages(connection)
            if len(messages) == 1 and not messages[0]:
                break
            for message in messages:
                if not message:
                    break
                self.match_message(connection, messages, message)

    def match_message(self, connection, messages, message):
        match message:
            case {"type": "adjust_replicas", "file_name": file_name, "number_of_replicas": number_of_replicas}:
                self.check_replicas(connection, file_name, number_of_replicas)
            case {"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content}:
                self.handle_store_file(user_id, file_name, file_content.encode("utf-8"))
            case {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}:
                self.handle_retrieve_file(connection, file_name, user_id)
            case {"type": "check_replicas", "file_name": file_name, "user_id": user_id}:
                self.check_replicas(connection, file_name, user_id)
            case {"type": "remove_replica", "file_name": file_name, "user_id": user_id}:
                self.remove_replica(connection, file_name, user_id)
            case {"type": "OK"}:
                _print_receive(messages)
            case _:
                print("Mensagem inválida")

    def remove_replica(self, connection, file_name: str, user_id: str):
        file_path = f"store/{self.bucket_id}/{user_id}/{file_name}"
        if os.path.exists(file_path):
            os.remove(file_path)
            self.send_ok(connection, "gateway")
        else:
            self.send_error(connection, "gateway")

    def handle_retrieve_file(self, connection, file_name, user_id):
        print_info(f"Enviando arquivo {file_name} para o usuário {user_id}...")
        file_content = self.maybe_get_file(user_id, file_name)
        if not file_content:
            self.send_error(connection, "gateway")
            return
        data = {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}
        self.send_message(connection, data, "gateway")

    def check_replicas(self, connection, file_name: str, user_id: str):
        file = self.maybe_get_file(user_id, file_name)
        self.send_message(connection, {"type": "check_replicas", "file_name": file_name, "user_id": user_id, "file_content": file}, "gateway")

    def handle_store_file(self, user_id: str, file_name: str, file_content: bytes):
        directory_path = f"store/{self.bucket_id}/{user_id}"
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, file_name)
        with open(file_path, "wb") as file:
            file.write(file_content)
        print_info(f"User: {user_id} Armazenou arquivo {file_name} com sucesso.")

    def maybe_get_file(self, user_id: str, file_name: str) -> bytes:
        file_path = f"store/{self.bucket_id}/{user_id}/{file_name}"
        if os.path.exists(file_path):
            with open(file_path, "rb") as file:
                print_info(f"Recuperando arquivo {file_name} para o usuário {user_id}...")
                return file.read().decode("utf-8")
        else:
            print_info(f"Arquivo {file_name} não encontrado.")
            return b"".decode("utf-8")

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
    Bucket(sys.argv[1])
