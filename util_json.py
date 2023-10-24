# util_json.py
import json
import socket
import base64


def enviar_json(conexao, dados):
    if "conteudo" in dados:
        dados["conteudo"] = base64.b64encode(dados["conteudo"]).decode("utf-8")
    mensagem_json = json.dumps(dados)
    conexao.sendall(mensagem_json.encode("utf-8"))


def receber_json(conexao, tamanho_buffer=1024):
    mensagem_codificada = conexao.recv(tamanho_buffer)
    mensagem_json = mensagem_codificada.decode("utf-8")
    dados = json.loads(mensagem_json)
    if "conteudo" in dados:
        dados["conteudo"] = base64.b64decode(dados["conteudo"].encode("utf-8"))
    return dados
