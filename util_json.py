# util_json.py
import json
import base64


def enviar_json(conexao, dados):
    if "conteudo" in dados:
        dados["conteudo"] = base64.b64encode(dados["conteudo"]).decode("utf-8")
    mensagem_json = json.dumps(dados)
    conexao.sendall(mensagem_json.encode("utf-8"))
