from saida_grafo.ConexaoSaida import ajustar_placeholders_para_neo4j, executar_cypher_batch, executar_cypher_multi_params, get_driver
from utils.TratarTiposValores import tratar_decimais

# def gerar_saida_de_nos(cypher_commands, valores_banco):
#     print("Inicio processo saida_nos")
#     driver = get_driver()

#     for no in cypher_commands:
#         nome_tabela = no["tabela"]
#         esquema_tabela = no["esquema"]

#         # Procurar os valores correspondentes no 'valores_banco'
#         tabela_valor = next(
#             (item for item in valores_banco
#              if item["tabela"] == nome_tabela and item["esquema"] == esquema_tabela),
#             None
#         )

#         if tabela_valor:
#             parametros = tabela_valor.get("parametros", [])
#             comando_cypher = ajustar_placeholders_para_neo4j(no["cypher"])
#             parametros_formatados = []
#             #parametros_formatados = tratar_decimais(parametros)
#             executar_cypher_multi_params(driver, comando_cypher, parametros)
        
#     print("Fim processo saida_nos")

#     driver.close()

# def gerar_saida_de_nos(cypher_commands, valores_banco):
#     print("Inicio processo saida_nos")
#     driver = get_driver()

#     for no in cypher_commands:
#         nome_tabela = no["tabela"]
#         esquema_tabela = no["esquema"]

#         tabela_valor = next(
#             (item for item in valores_banco
#              if item["tabela"] == nome_tabela and item["esquema"] == esquema_tabela),
#             None
#         )

#         if tabela_valor:
#             parametros = tabela_valor.get("parametros", [])
#             if not parametros:
#                 continue

#             comando_cypher = ajustar_placeholders_para_neo4j(no["cypher"])
#             executar_cypher_batch_unwind(driver, f"UNWIND $lote AS row {comando_cypher}", parametros)

#     print("Fim processo saida_nos")
#     driver.close()

# def executar_cypher_batch_unwind(driver, cypher, parametros_list, batch_size=10000):
#     with driver.session() as session:
#         for i in range(0, len(parametros_list), batch_size):
#             batch = parametros_list[i:i + batch_size]
#             session.run(cypher, {"lote": batch})


def gerar_saida_de_nos(cypher_commands, valores_banco, tamanho_lote=5000):
    print("Inicio processo saida_nos")
    driver = get_driver()

    for no in cypher_commands:
        nome_tabela = no["tabela"]
        esquema_tabela = no["esquema"]
        label = nome_tabela  # você pode customizar isso se quiser

        # Procurar os valores correspondentes no 'valores_banco'
        tabela_valor = next(
            (item for item in valores_banco
             if item["tabela"] == nome_tabela and item["esquema"] == esquema_tabela),
            None
        )

        if tabela_valor:
            parametros = tabela_valor.get("parametros", [])
            if not parametros:
                continue

            # Divide em batches
            for i in range(0, len(parametros), tamanho_lote):
                batch = parametros[i:i + tamanho_lote]
                batch_tratado = tratar_decimais(batch)

                # Gera campos dinamicamente
                campos = batch[0].keys()
                propriedades = ', '.join([f"{campo}: row.{campo}" for campo in campos])

                cypher = f"""
                UNWIND $lote AS row
                CREATE (:{label} {{ {propriedades} }})
                """

                executar_cypher_batch(driver, cypher, batch_tratado)
        
    print("Fim processo saida_nos")
    driver.close()