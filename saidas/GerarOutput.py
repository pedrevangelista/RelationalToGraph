import base64
import datetime
from decimal import Decimal
import json
import os

def gerar_output(nome_banco, json_tabelas, cypher, relaciomentos_cypher, valores_banco_cypher):


    tabelas_info = json.dumps(json_tabelas, indent=4)

    arquivo = abrir_arquivo_saida(nome_banco, 'tabelas_infos.json', "w")
    arquivo.write(tabelas_info)
    arquivo.close()

    cypher_json = json.dumps(cypher, indent=4)

    arquivo = abrir_arquivo_saida(nome_banco, 'cypher.json', "w")
    arquivo.write(cypher_json)
    arquivo.close()

    relaciomentos_cypher_json = json.dumps(relaciomentos_cypher, indent=4)

    arquivo = abrir_arquivo_saida(nome_banco, 'relaciomentos_cypher_json.json', "w")
    arquivo.write(relaciomentos_cypher_json)
    arquivo.close()

def gerar_output_json(nome_banco, result, file_name):

    result_json = json.dumps(result, default=data_converter, indent=4)

    arquivo = abrir_arquivo_saida(nome_banco, file_name, "w")
    arquivo.write(result_json)
    arquivo.close()

def gerar_output(nome_banco, result, file_name):

    arquivo = abrir_arquivo_saida(nome_banco, file_name, "w")
    arquivo.write(result)
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
