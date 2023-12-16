import socket
import json
import os
from utils import print_info, CommunicationManager


class Main(CommunicationManager):
    def __init__(self, user_id: str):
        super().__init__(f"Client: {user_id}")
        self.user_id = user_id
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect(("localhost", 12345))

        while True:
            modo = input("Escolha o modo (deposito/recuperacao/sair): ")
            number_of_replicas = int(input("Numero de replicas:"))
            file_name = input("Digite o nome do arquivo: ")

            match modo:
                case "deposito":
                    self.handle_deposito(user_id, connection, number_of_replicas, file_name)
                    break  # REMOVERDEPOIS

                case "recuperacao":
                    self.handle_recuperacao(user_id, connection, file_name)
                    break  # REMOVERDEPOIS

                case "ajustar_replicas":
                    self.handle_ajustar_replicas(connection, file_name, number_of_replicas)
                    break

                case "sair":
                    print("Saindo...")
                    break

        connection.close()

    def handle_recuperacao(self, user_id, connection, file_name):
        dados = {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}
        self.send_message(connection, dados, "gateway")
        content = self.receive_messages(connection)[0]
        if not content:
            print_info("Arquivo não encontrado")
            return
        # info_print(f"Arquivo recuperado com sucesso: {file_name} {len(content.file_content)}")
        # Salva o arquivo recuperado
        # Cria o diretório se ele não existir
        directory_path = f"meus_arquivos/{user_id}"
        os.makedirs(directory_path, exist_ok=True)

        # # Salva o arquivo recuperado
        with open(f"{directory_path}/{file_name}", "wb") as arquivo:
            arquivo.write(content["file_content"].encode("utf-8"))
        print_info(f"Arquivo recuperado com sucesso: {file_name} {len(content['file_content'])}")

    def handle_deposito(self, user_id, connection, number_of_replicas, file_name):
        with open(file_name, "rb") as arquivo:
            file_content = arquivo.read()
        dados = {
            "type": "store_file",
            "user_id": user_id,
            "file_name": file_name,
            "file_content": file_content.decode("utf8"),
            "number_of_replicas": number_of_replicas,
        }
        self.send_message(connection, dados, "gateway")
        self.receive_messages(connection)

    def handle_ajustar_replicas(self, connection, file_name, number_of_replicas):
        dados = {"type": "adjust_replicas", "file_name": file_name, "number_of_replicas": number_of_replicas, "user_id": self.user_id}
        self.send_message(connection, dados, "gateway")
        response = self.receive_messages(connection)
        match response:
            case {
                "type": "OK",
            }:
                print_info("Réplicas ajustadas")
            case {"type": "ERROR", "message": message}:
                print_info(f"Erro ao ajustar réplicas: {message}")
            case _:
                print_info("Mensagem inválida")

    def disconnect(self, connection: socket.socket):
        self.send_message(connection, {"type": "client_disconnected", "user_id": self.user_id}, "gateway")
        connection.close()


if __name__ == "__main__":
    # main(sys.argv[1])
    Main("tony")
