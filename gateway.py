import threading
from typing import Dict
import socket
import json
from utils import print_info, print_receive, CommunicationManager


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
                response = connection.recv(1024).decode("utf-8")
                print_receive(f"Recebido: {response}")
                if not response:
                    break  # Encerra o loop se não houver dados (conexão fechada)

                match json.loads(response):
                    case {"type": "subscribe_bucket", "bucket_id": bucket_id}:
                        print_info(f"{address} conectado como bucket | bucket_id: {bucket_id} | Thread: {threading.get_ident() % 9}")
                        self.buckets[bucket_id] = connection
                        continue
                    case {"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content, "number_of_replicas": _number_of_replicas}:
                        # Nesse caminho o gateway recebe a primeira mensagem do cliente
                        client_connection = connection
                        buckets = list(self.buckets.values())
                        # O máximo de réplicas é limitado pelo número de buckets disponíveis
                        picked_buckets = buckets[: min(_number_of_replicas, len(buckets))]
                        for bucket in picked_buckets:
                            data = json.dumps({"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content})
                            self.send_message(bucket, json.loads(data), "bucket")

                        self.send_ok(client_connection, "Client")
                        continue
                    case {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}:
                        print_info(f"Buscando arquivo {file_name} para o usuário {user_id}...")
                        self.clients[user_id] = connection
                        buckets = list(self.buckets.values())
                        # Pergunta aos buckets quem tem arquivos do seguinte usuário
                        for bucket in buckets:
                            data = {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}
                            self.send_message(bucket, data, "bucket")
                            continue
                    case {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}:
                        # Ack a mensagem do bucket
                        self.send_ok(connection, "bucket")
                        data = {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}
                        client_connection = self.clients[user_id]
                        self.send_message(client_connection, data, "Client")
                        continue
                    case {"type": "client_disconnected", "user_id": user_id}:
                        self.clients.pop(user_id)
                        continue
                    case _:
                        print("Mensagem inválida")
                        self.send_error(connection, "UNKNOWN")

            except json.JSONDecodeError:
                print("Erro ao decodificar JSON")
                break

        connection.close()


if __name__ == "__main__":
    Gateway()
