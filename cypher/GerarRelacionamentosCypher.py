from utils.Geral import formatar_coluna

def gerar_relacionamentos_cypher(tabelas):
    cypher_rels = []

    # Mapeia tabelas por nome para lookup rápido
    mapa_tabelas = {
        (t["esquema"], t["nome_tabela"]): t for t in tabelas
    }
    for tabela in tabelas:
        if tabela.get("aninhamento", 0) == 1:
            continue  # Ignora tabelas aninhadas

        nome_tabela_filha = tabela["nome_tabela"]
        esquema_filha = tabela["esquema"]
        for fk in tabela.get("chaves_estrangeiras", []):
            nome_tabela_pai = fk["tabela_referenciada"]
            esquema_pai = fk["schema_referenciado"]
            nome_destino = fk["tabela_referenciada"]
            esquema_destino = fk["schema_referenciado"]

            # Verifica se a tabela de destino existe E se não é aninhada
            chave_destino = (esquema_destino, nome_destino)
            tabela_destino = mapa_tabelas.get(chave_destino)

            if not tabela_destino or tabela_destino.get("aninhamento", 0) == 1:
                continue

            # Nome das colunas formatado sem espaços e com PascalCase
            coluna_filha = formatar_coluna(fk["coluna"])
            coluna_pai = formatar_coluna(fk["coluna_referenciada"])

            cypher = (
                f"MATCH (a:{nome_tabela_filha} {{ {coluna_filha}: ${coluna_filha} }})\n"
                f"MATCH (b:{nome_tabela_pai} {{ {coluna_pai}: ${coluna_filha} }})\n"
                f"CREATE (a)-[:PERTENCE_A]->(b);"
            )

            cypher_rels.append({
                "tabela_origem": nome_tabela_filha,
                "tabela_destino": nome_tabela_pai,
                "esquema_origem": esquema_filha,
                "esquema_destino": esquema_pai,
                "cypher": cypher
            })

    return cypher_rels

def gerar_relacionamentos_cypher_dados(dados_arestas):
    cypher_rels = []

    for aresta in dados_arestas:
        tabela_origem = aresta["sourceLabel"]
        tabela_destino = aresta["targetLabel"]
        coluna_origem = aresta["source"]
        coluna_destino = aresta["target"]

        valor_origem = aresta["properties"].get(coluna_origem)
        valor_destino = aresta["properties"].get(coluna_destino)

        cypher = (
            f"MATCH (a:{tabela_origem} {{ {coluna_origem}: '{valor_origem}' }})\n"
            f"MATCH (b:{tabela_destino} {{ {coluna_destino}: '{valor_destino}' }})\n"
            f"CREATE (a)-[:PERTENCE_A]->(b);"
        )

        cypher_rels.append({
            "tabela_origem": tabela_origem,
            "tabela_destino": tabela_destino,
            "cypher": cypher
        })

    return cypher_rels