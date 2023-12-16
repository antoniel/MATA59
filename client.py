import socket
import os
from utils import print_info, CommunicationManager


class Main(CommunicationManager):
    def __init__(self, user_id: str):
        super().__init__(f"Client 📱: {user_id}")
        self.user_id = user_id
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect(("localhost", 12345))

        while True:
            # modo = input("Escolha o modo (deposito/recuperacao/sair): ")
            modo = "recuperacao"
            # number_of_replicas = input("Numero de replicas:")
            number_of_replicas = 1
            # file_name = input("Digite o nome do arquivo: ")
            file_name = "some.txt"

            match modo:
                case "deposito":
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
                    break  # REMOVERDEPOIS

                case "recuperacao":
                    dados = {"type": "retrieve_file", "user_id": user_id, "file_name": file_name}
                    self.send_message(connection, dados, "gateway")
                    content = self.receive_message(connection)
                    # info_print(f"Arquivo recuperado com sucesso: {file_name} {len(content.file_content)}")
                    # Salva o arquivo recuperado
                    # Cria o diretório se ele não existir
                    directory_path = f"meus_arquivos/{user_id}"
                    os.makedirs(directory_path, exist_ok=True)

                    # # Salva o arquivo recuperado
                    with open(f"{directory_path}/{file_name}", "wb") as arquivo:
                        arquivo.write(content["file_content"].encode("utf-8"))
                    print_info(f"Arquivo recuperado com sucesso: {file_name} {len(content['file_content'])}")
                    break  # REMOVERDEPOIS
                case "sair":
                    print("Saindo...")
                    break

        connection.close()

    def disconnect(self, connection: socket.socket):
        self.send_message(connection, {"type": "client_disconnected", "user_id": self.user_id}, "gateway")
        connection.close()


if __name__ == "__main__":
    # main(sys.argv[1])
    Main("dib")
