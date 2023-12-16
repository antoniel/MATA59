import threading
from typing import Dict, Any
import socket
import json
from utils import print_info, CommunicationManager


class Gateway(CommunicationManager):
    buckets: Dict[str, socket.socket] = {}
    bucketsListeners: Dict[str, Dict[str, Any]] = {}
    clients: Dict[str, socket.socket] = {}

    def __init__(self):
        super().__init__("Gateway")
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
                        case {"type": "subscribe_bucket", "bucket_id": bucket_id, "address": address, "port": port}:
                            self.handle_subscribe_bucket(bucket_id, address, port, connection)
                        case {"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content, "number_of_replicas": _number_of_replicas}:
                            self.handle_store_file(connection, user_id, file_name, file_content, _number_of_replicas)
                        case {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}:
                            self.handle_retrieve_file(user_id, file_name, connection)
                        case {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}:
                            self.handle_retrieve_file_response(user_id, file_name, file_content, connection)
                        case {"type": "adjust_replicas", "file_name": file_name, "number_of_replicas": number_of_replicas, "user_id": user_id}:
                            self.handle_adjust_replicas(connection, file_name, number_of_replicas, user_id)
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

    def handle_subscribe_bucket(self, bucket_id, address, port, connection):
        self.buckets[bucket_id] = connection
        self.bucketsListeners[bucket_id] = {"address": address, "port": port}
        print_info(f"{address} conectado como bucket | bucket_id: {bucket_id} | Thread: {threading.get_ident()}")

    def handle_store_file(self, connection, user_id, file_name, file_content, _number_of_replicas):
        client_connection = connection
        buckets = list(self.buckets.values())
        # O máximo de réplicas é limitado pelo número de buckets disponíveis
        picked_buckets = buckets[: min(_number_of_replicas, len(buckets))]
        for bucket in picked_buckets:
            self.store_file(connection=bucket, user_id=user_id, file_name=file_name, file_content=file_content, _number_of_replicas=_number_of_replicas)

        self.send_ok(client_connection, "Client")

    def store_file(self, connection, user_id, file_name, file_content, _number_of_replicas):
        data = {"type": "store_file", "user_id": user_id, "file_name": file_name, "file_content": file_content}
        self.send_message(connection, (data), "Bucket")

    def handle_retrieve_file(self, user_id, file_name, connection):
        # Implemente a lógica específica para "retrieve_file"
        print_info(f"Buscando arquivo {file_name} para o usuário {user_id}...")
        self.clients[user_id] = connection
        buckets = list(self.buckets.values())
        # Pergunta aos buckets quem tem arquivos do seguinte usuário
        for bucket in buckets:
            data = {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}
            self.send_message(bucket, data, "Bucket")

    def handle_retrieve_file_response(self, user_id, file_name, file_content, connection):
        self.send_ok(connection, "bucket")
        data = {"type": "retrieve_file_response", "user_id": user_id, "file_name": file_name, "file_content": file_content}
        client_connection = self.clients[user_id]
        self.send_message(client_connection, data, "Client")

    def handle_adjust_replicas(self, client_connection, file_name, expected_number_of_replicas, user_id):
        replica_status = {}
        for bucket_id, bucket_conn in self.bucketsListeners.items():
            bucket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            bucket_connection.connect((bucket_conn["address"], bucket_conn["port"]))
            self.send_message(bucket_connection, {"type": "check_replicas", "file_name": file_name, "user_id": user_id}, "Bucket")
            response = self.receive_messages(bucket_connection)[0]
            match response:
                case {"type": "check_replicas", "file_name": file_name, "user_id": user_id, "file_content": file}:
                    replica_status[bucket_id] = {"file_content": file, "file_name": file_name, "user_id": user_id, "bucket_id": bucket_id}
                case _:
                    replica_status[bucket_id] = None
                    continue

        curent_replicas = list(filter(lambda x: bool(x["file_content"]), replica_status.values()))
        print_info(f"Réplicas atuais: {len(curent_replicas)}")
        print_info(f"Réplicas esperadas: {(expected_number_of_replicas)}")
        print_info(f"Número de buckets disponíveis: {len(self.buckets.values())}")

        if len(curent_replicas) == 0:
            print_info(f"O Arquivo {file_name} não existe em nenhum bucket, você precisa depositar ele primeiro para ajustar as réplicas.")
            self.send_message(client_connection, {"type": "ERROR", "message": "Esse arquivo não existe"}, "Client")
            return

        if expected_number_of_replicas == len(curent_replicas):
            print_info("Não é necessário ajustar réplicas")
            return

        if expected_number_of_replicas > len(curent_replicas):
            sample_replica = curent_replicas[0]
            print_info("Adicionando réplicas")
            # Adiciona as réplicas
            for bucket_id, bucket_conn in self.bucketsListeners.items():
                if bucket_id in map(lambda x: x["bucket_id"], curent_replicas):
                    continue
                bucket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                bucket_connection.connect((bucket_conn["address"], bucket_conn["port"]))
                self.store_file(bucket_connection, sample_replica["user_id"], sample_replica["file_name"], sample_replica["file_content"], 1)
                bucket_connection.close()

            self.send_ok(client_connection, "Client")
            return

        if expected_number_of_replicas <= len(curent_replicas):
            maxmimum_items_to_remove = len(curent_replicas) - expected_number_of_replicas
            for bucket_id, bucket_conn in self.bucketsListeners.items():
                if maxmimum_items_to_remove == 0:
                    break
                maxmimum_items_to_remove -= 1
                bucket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                bucket_connection.connect((bucket_conn["address"], bucket_conn["port"]))
                self.send_message(bucket_connection, {"type": "remove_replica", "file_name": file_name, "user_id": user_id}, "Bucket")
                self.receive_messages(bucket_connection)
                bucket_connection.close()

            self.send_ok(client_connection, "Client")

        # Eu preciso saber das outra Threads se elas tem o arquivo
        # Sabendo quem tem e quem não tem, eu posso decidir se eu preciso replicar ou remover
        # Porém como eu faço isso?

        # Coleta as respostas
        # responses = []
        # for _ in range(len(self.buckets)):
        #     response = self.response_queue.get()  # Bloqueia até que uma resposta seja colocada na fila
        #     responses.append(response)


if __name__ == "__main__":
    Gateway()
