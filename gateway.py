import socket
from typing import Dict
from messages import BucketSubscribeMessage, ClientGetFile
import json


class Gateway:
    connection: socket.socket
    buckets: Dict[str, BucketSubscribeMessage] = {}

    def __init__(self):
        gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gateway_socket.bind(("localhost", 12345))
        gateway_socket.listen(5)
        print(f"Servidor pronto para conexões. em {gateway_socket.getsockname()}")

        while True:
            self.connection, address = gateway_socket.accept()
            data = json.loads(self.connection.recv(1024).decode("utf-8"))
            match data:
                case {"type": "subscribe_bucket"}:
                    bucket = BucketSubscribeMessage(address=address, bucket_id=data["bucket_id"])
                    print(f"{address} Subscribing as bucket | bucket_id: {bucket.bucket_id}")
                    self.buckets[bucket.bucket_id] = bucket
                    self.OK()
                case {"type": "subscribe_client"}:
                    client = ClientGetFile(address=address, user_id=data["user_id"])
                    print(f"{address} Subscribing as client | user_id: {client.user_id}")
                    self.OK()
                case _:
                    print("Invalid message")
                    self.ERROR()

            self.connection.close()

    def OK(self):
        self.connection.send("OK".encode("utf-8"))

    def ERROR(self):
        self.connection.send("ERROR".encode("utf-8"))


if __name__ == "__main__":
    Gateway()
