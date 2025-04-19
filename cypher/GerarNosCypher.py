import re

from utils.Geral import formatar_coluna

# def gerar_nos_cypher(tabelas):
#     cypher_nos = []

#     for tabela in tabelas:
#         if tabela.get("aninhamento", 0) == 1:
#             continue  # Ignora as que serão aninhadas

#         nome_tabela = tabela["nome_tabela"]
#         esquema = tabela["esquema"]
#         propriedades = [coluna["nome"] for coluna in tabela["colunas"]]

#         props_string = ", ".join(
#             [f"{formatar_coluna(prop)}: ${{{formatar_coluna(prop)}}}" for prop in propriedades]
#         )
#         cypher = f"CREATE (:{nome_tabela} {{ {props_string} }});"

#         cypher_nos.append({
#             "tabela": nome_tabela,
#             "esquema": esquema,
#             "cypher": cypher
#         })

#     return cypher_nos

def gerar_nos_cypher_dados(dados_nos):
    cypher_nos = []

    for tabela in dados_nos:

        nome_tabela = tabela["type"]
        esquema = tabela["scheme"]
        propriedades = [f"{chave}={valor}" for chave, valor in tabela["properties"].items()]
        props_dict = {
            item.split('=')[0]: item.split('=')[1] for item in propriedades
        }
        props_string = ", ".join([f"{chave}: '{valor}'" for chave, valor in props_dict.items()])

        cypher = f"CREATE (:{nome_tabela} {{ {props_string} }});"

        cypher_nos.append({
            "tabela": nome_tabela,
            "esquema": esquema,
            "cypher": cypher
        })

    return cypher_nos