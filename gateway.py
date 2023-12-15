import threading
import socket
from typing import Dict
import json


class Gateway:
    buckets: Dict[str, socket.socket] = {}

    def __init__(self):
        self.buckets = {}
        self.gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
                print(f"Recebido: {response}")
                if not response:
                    break  # Encerra o loop se não houver dados (conexão fechada)

                match json.loads(response):
                    case {"type": "subscribe_bucket", "bucket_id": bucket_id}:
                        print(f"{address} conectado como bucket | bucket_id: {bucket_id}")
                        self.buckets[bucket_id] = connection
                        # Print the number of current threads
                    case {
                        "type": "store_file",
                        "user_id": user_id,
                        "file_name": file_name,
                        "file_content": file_content,
                        "number_of_replicas": _number_of_replicas,
                    }:
                        buckets = list(self.buckets.values())
                        # Max of replicas should be the number of buckets
                        picked_buckets = buckets[: min(_number_of_replicas, len(buckets))]
                        for bucket in picked_buckets:
                            bucket.send(
                                json.dumps(
                                    {
                                        "type": "store_file",
                                        "user_id": user_id,
                                        "file_name": file_name,
                                        "file_content": file_content,
                                    }
                                ).encode("utf-8")
                            )
                        connection.send("OK".encode("utf-8"))
                    case _:
                        print("Mensagem inválida")
                        self.send_error(connection)
            except json.JSONDecodeError:
                print("Erro ao decodificar JSON")
                break
        connection.close()

    def send_ok(self, connection: socket.socket):
        connection.send("OK".encode("utf-8"))

    def send_error(self, connection: socket.socket):
        connection.send("ERROR".encode("utf-8"))


if __name__ == "__main__":
    Gateway()
