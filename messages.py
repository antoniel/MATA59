from typing import Literal
import json


class Message:
    type: Literal["subscribe_bucket", "subscribe_client", "get_file", "store_file", "exit"]
    address: str

    def encode(self):
        return json.dumps(self.__dict__).encode("utf-8")


# echo '{"type":"subscribe_bucket", "bucket_id": "s1"}' | nc localhost 12345
class BucketSubscribeMessage(Message):
    bucket_id: str

    def __init__(self, bucket_id: str, address):
        self.type = "subscribe_bucket"
        self.address = address
        self.bucket_id = bucket_id


# echo '{}' | nc localhost 12345
class ClientGetFile(Message):
    user_id: str

    def __init__(self, user_id: str, address):
        self.type = "get_file"
        self.address = address
        self.user_id = user_id
