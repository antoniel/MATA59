import socket
import time
import json
from typing import Union, List, Dict, Any
import threading

DEBUG = True
INFO = True


def debug_print(*args):
    if DEBUG:
        print(*args)


def _send_print(*args):
    if DEBUG:
        print("\033[92m", end="")  # Inicia a cor verde
        print(*args, end="")  # Imprime os argumentos sem nova linha ao final
        print("\033[0m")  # Reseta a cor para o padrão


def _print_receive(*args):
    if DEBUG:
        print("\033[93m", end="")  # Inicia a cor amarela
        print(*args, end="")  # Imprime os argumentos sem nova linha ao final
        print("\033[0m")  # Reseta a cor para o padrão


def print_info(*args):
    if INFO:
        print("\033[94m", end="")
        print(*args, end="")
        print("\033[0m")


class CommunicationManager:
    def __init__(self, serviceName: str):
        # Você pode adicionar inicializações adicionais aqui, se necessário
        self.service_name = serviceName

    def send_message(self, connection: socket.socket, message: dict, to: str):
        message["From"] = self.service_name
        message["To"] = to
        _send_print(f"{str(threading.get_ident())[-2:]} | Enviando: {message}")
        connection.send((json.dumps(message) + "\n").encode("utf-8"))

    def receive_messages(self, connection: socket.socket) -> List[Union[None, Dict[str, Any]]]:
        time.sleep(0.1)
        response = connection.recv(1024).decode("utf-8")

        _print_receive(f"{str(threading.get_ident())[-2:]} | Recebido: {response}")
        if not response:
            return [None]

        # Separar as mensagens usando splitlines()
        messages = response.splitlines()

        # Filtrar as mensagens vazias e decodificar cada mensagem JSON
        decoded_messages = [json.loads(message) for message in messages if message]

        return decoded_messages

    def send_ok(self, connection: socket.socket, to: str):
        self.send_message(connection, {"type": "OK"}, to)

    def send_error(self, connection: socket.socket, to: str):
        self.send_message(connection, {"type": "ERROR"}, to)
