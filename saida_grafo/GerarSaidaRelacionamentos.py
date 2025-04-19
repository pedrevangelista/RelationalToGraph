import re
from saida_grafo.ConexaoSaida import ajustar_placeholders_para_neo4j, executar_cypher_batch, executar_cypher_multi_params, get_driver
from utils.TratarTiposValores import tratar_decimais


def gerar_saida_relacionamentos(relacionamentos, valores_banco, tamanho_lote = 5000):
    print("Inicio processo saida_relacionamento")
    driver = get_driver()

    for rel in relacionamentos:
        tabela_origem = rel["tabela_origem"]
        esquema_origem = rel.get("esquema_origem", "dbo")  # padrão, se quiser
        label = tabela_origem  # você pode customizar isso se quiser

        cypher = ajustar_placeholders_para_neo4j(rel["cypher"])
        cypher_com_row = re.sub(r"\$(\w+)", r"row.\1", cypher)

        # Busca os parâmetros da tabela de origem
        tabela_valor_origem = next(
            (item for item in valores_banco
             if item["tabela"] == tabela_origem and item["esquema"] == esquema_origem),
            None
        )
        if tabela_valor_origem:
            parametros_origem = tabela_valor_origem.get("parametros", [])
            parametros = tratar_decimais(parametros_origem)
            # Dividir em batches e executar
            for i in range(0, len(parametros), tamanho_lote):
                batch = parametros[i:i + tamanho_lote]
                batch_tratado = tratar_decimais(batch)

                cypher_tratado = f"""
                UNWIND $lote AS row
                {cypher_com_row}
                """
                
                executar_cypher_batch(driver, cypher_tratado, batch_tratado)
            #executar_cypher_multi_params(driver, cypher, parametros_origem)
        else:
            print(f"[AVISO] Nenhum valor encontrado para {tabela_origem}.{esquema_origem}")
        
    print("Fim processo saida_relacionamento")
    driver.close()

# def gerar_saida_relacionamentos(relacionamentos, valores_banco, tamanho_lote=1000):
#     print("Inicio processo saida_relacionamento")
#     driver = get_driver()

#     for rel in relacionamentos:
#         tabela_origem = rel["tabela_origem"]
#         tabela_destino = rel["tabela_destino"]
#         esquema_origem = rel.get("esquema_origem", "dbo")
#         esquema_destino = rel.get("esquema_destino", "dbo")

#         cypher_base = ajustar_placeholders_para_neo4j(rel["cypher"])

#         # Buscar parâmetros
#         tabela_valor_origem = next(
#             (item for item in valores_banco
#              if item["tabela"] == tabela_origem and item["esquema"] == esquema_origem),
#             None
#         )

#         tabela_valor_destino = next(
#             (item for item in valores_banco
#              if item["tabela"] == tabela_destino and item["esquema"] == esquema_destino),
#             None
#         )

#         if not (tabela_valor_origem and tabela_valor_destino):
#             print(f"[AVISO] Dados não encontrados para {tabela_origem}.{esquema_origem} ou {tabela_destino}.{esquema_destino}")
#             continue

#         # Unir os parâmetros das duas tabelas (merge por chave se necessário)
#         parametros_origem = tabela_valor_origem.get("parametros", [])
#         parametros_destino = tabela_valor_destino.get("parametros", [])

#         # Criar um dicionário indexado pelas chaves primárias para fazer o merge
#         origem_index = {tuple(item[k] for k in item if k.endswith("ID")): item for item in parametros_origem}
#         destino_index = {tuple(item[k] for k in item if k.endswith("ID")): item for item in parametros_destino}

#         # Interseção por chaves
#         chaves_comuns = set(origem_index.keys()) & set(destino_index.keys())

#         dados_unificados = []
#         for chave in chaves_comuns:
#             combinado = {**origem_index[chave], **destino_index[chave]}
#             dados_unificados.append(combinado)

#         if not dados_unificados:
#             print(f"[AVISO] Nenhum relacionamento válido encontrado para {tabela_origem} → {tabela_destino}")
#             continue

#         # Dividir em batches e executar
#         for i in range(0, len(dados_unificados), tamanho_lote):
#             batch = dados_unificados[i:i + tamanho_lote]
#             batch_tratado = tratar_decimais(batch)

#             cypher = f"""
#             UNWIND $lote AS row
#             {cypher_base}
#             """
#             executar_cypher_batch(driver, cypher, batch_tratado)

#     print("Fim processo saida_relacionamento")
#     driver.close()