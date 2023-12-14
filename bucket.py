import socket
import json
from gateway import BucketSubscribeMessage


class Bucket:
    connection: socket.socket
    bucket_id: str

    def __init__(self, bucket_id: str):
        address = self.subscribe_as_bucket(bucket_id)
        print(f"Subscribed as bucket | bucket_id: {bucket_id} | address: {address}")
        self.listen_for_requests(address)

    def subscribe_as_bucket(self, bucket_id):
        bucket_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bucket_socket.connect(("localhost", 12345))
        bucket_socket.sendall(BucketSubscribeMessage(bucket_id=bucket_id, address=bucket_socket.getsockname()).encode())
        address = bucket_socket.getsockname()
        bucket_socket.close()
        return address

    def listen_for_requests(self, address):
        bucket_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Configura o socket para reutilizar o endereço/porta imediatamente
        bucket_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bucket_socket.bind(address)
        bucket_socket.listen(1)
        print(f"Servidor pronto para conexões. em {bucket_socket.getsockname()}")
        self.connection, address = bucket_socket.accept()
        data = json.loads(self.connection.recv(1024).decode("utf-8"))
        match data:
            case {"type": "subscribe_bucket"}:
                bucket = BucketSubscribeMessage(address=address, bucket_id=data["bucket_id"])
                print(f"{address} Subscribing as bucket | bucket_id: {bucket.bucket_id}")
            case _:
                print("Invalid message")

        self.connection.close()

    def store_file(self, user_id: str, file_name: str):
        print(f"User: {user_id} Armazenando arquivo {file_name}...")

    def retrieve_file(self, file_name: str) -> bytes:
        print(f"Recuperando arquivo {file_name}...")


if __name__ == "__main__":
    Bucket("s1")
