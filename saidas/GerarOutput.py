import base64
import datetime
from decimal import Decimal
import json
import os

def gerar_output_json(nome_banco, result, file_name):

    result_json = json.dumps(result, default=data_converter, indent=4)

    arquivo = abrir_arquivo_saida(nome_banco, file_name, "w")
    arquivo.write(result_json)
    arquivo.close()

    
def abrir_arquivo_saida(nome_banco: str, nome_arquivo: str, modo: str = "w", encoding: str = "utf-8"):
    pasta = os.path.join("saidas", nome_banco)
    os.makedirs(pasta, exist_ok=True)
    caminho_arquivo = os.path.join(pasta, nome_arquivo)
    return open(caminho_arquivo, modo, encoding=encoding)

def data_converter(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('utf-8')
    if isinstance(obj, Decimal):
        return float(obj) 
    raise TypeError(f"Type {obj.__class__.__name__} não é serializável")
