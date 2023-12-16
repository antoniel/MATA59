import threading
from typing import Dict
import socket
import json
from utils import print_info, CommunicationManager


class Gateway(CommunicationManager):
    buckets: Dict[str, socket.socket] = {}
    clients: Dict[str, socket.socket] = {}

    def __init__(self):
        super().__init__("Gateway 🌐")
        self.buckets = {}
        self.clients = {}
        self.gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.gateway_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.gateway_socket.bind(("localhost", 12345))
        self.gateway_socket.listen(8)
        print(f"Servidor pronto para conexões em {self.gateway_socket.getsockname()}")
        while True:
            connection, address = self.gateway_socket.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(connection, address))
            client_thread.start()

    def handle_client(self, connection, address):
        while True:
            try:
                messages = self.receive_messages(connection)
                if len(messages) == 1 and not messages[0]:
                    break
                for message in messages:
                    if not message:
                        break  # Encerra o loop se não houver dados (conexão fechada)

                    match message:
                        case {"type": "subscribe_bucket", "bucket_id": bucket_id}:
                            self.handle_subscribe_bucket(bucket_id, address, connection)
                        case {"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content, "number_of_replicas": _number_of_replicas}:
                            self.handle_store_file(user_id, file_name, file_content, _number_of_replicas, connection)
                        case {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}:
                            self.handle_retrieve_file(user_id, file_name, connection)
                        case {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}:
                            self.handle_retrieve_file_response(user_id, file_name, file_content, connection)
                        case {"type": "adjust_replicas", "file_name": file_name, "number_of_replicas": number_of_replicas}:
                            self.handle_adjust_replicas(file_name, number_of_replicas)
                        case {"type": "client_disconnected", "user_id": user_id}:
                            self.clients.pop(user_id)
                            continue
                        case {"type": "ERROR"}:
                            continue
                        case _:
                            print("Mensagem inválida")
                            self.send_error(connection, "UNKNOWN")

            except json.JSONDecodeError:
                print("Erro ao decodificar JSON")
                break

        connection.close()

    def handle_subscribe_bucket(self, bucket_id, address, connection):
        print_info(f"{address} conectado como bucket | bucket_id: {bucket_id} | Thread: {threading.get_ident() % 9}")
        self.buckets[bucket_id] = connection

    def handle_store_file(self, user_id, file_name, file_content, _number_of_replicas, connection):
        client_connection = connection
        buckets = list(self.buckets.values())
        # O máximo de réplicas é limitado pelo número de buckets disponíveis
        picked_buckets = buckets[: min(_number_of_replicas, len(buckets))]
        for bucket in picked_buckets:
            data = json.dumps({"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content})
            self.send_message(bucket, json.loads(data), "bucket")

        self.send_ok(client_connection, "Client")

    def handle_retrieve_file(self, user_id, file_name, connection):
        # Implemente a lógica específica para "retrieve_file"
        print_info(f"Buscando arquivo {file_name} para o usuário {user_id}...")
        self.clients[user_id] = connection
        buckets = list(self.buckets.values())
        # Pergunta aos buckets quem tem arquivos do seguinte usuário
        for bucket in buckets:
            data = {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}
            self.send_message(bucket, data, "bucket")

    def handle_retrieve_file_response(self, user_id, file_name, file_content, connection):
        self.send_ok(connection, "bucket")
        data = {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}
        client_connection = self.clients[user_id]
        self.send_message(client_connection, data, "Client")

    def handle_adjust_replicas(self, file_name, number_of_replicas):
        allReplicas = []
        for bucket in self.buckets.values():
            data = {"type": "check_replicas", "file_name": file_name}
            self.send_message(bucket, data, "bucket")


if __name__ == "__main__":
    Gateway()
