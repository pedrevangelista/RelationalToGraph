def gerar_nos_grafo(estruturas, parametros_aninhados):
    vertices = []
    id_map = {}  # Mapeamento (tabela, id) -> índice no grafo

    # Passo 1: Criar vértices para todas as entidades principais
    for tabela in estruturas:
        if tabela["aninhamento"] == 0 and tabela["tipo"] != "N:N":
            dados_tabela = next(
                (p for p in parametros_aninhados 
                 if p["tabela"] == tabela["nome_tabela"] and p["esquema"] == tabela["esquema"]),
                None
            )
            
            if dados_tabela:
                for item in dados_tabela["parametros"]:
                    pk = tabela["chave_primaria"][0]  # Assume PK simples
                    vertex_id = f"{tabela['nome_tabela']}:{item[pk]}"
                    
                    vertices.append({
                        "id": vertex_id,
                        "type": tabela["nome_tabela"],
                        "scheme": tabela["esquema"],
                        "isVertex": True,
                        "properties": item
                    })
                    
                    # Registrar no mapa de IDs
                    id_map[(tabela["esquema"], tabela["nome_tabela"], item[pk])] = vertex_id
    return [vertices, id_map]

def gerar_nos_generico_deprecated(tabelas, valores_banco):
    nos = []

    for tabela in tabelas:
        nome_tabela = tabela["nome_tabela"]
        esquema = tabela.get("esquema", "dbo")
        colunas = tabela.get("colunas", [])
        chaves_estrangeiras = tabela.get("chaves_estrangeiras", [])
        grau_relacao = tabela.get("grau_relacao", 0)

        linhas = next(
            (t["parametros"] for t in valores_banco if t["tabela"] == nome_tabela and t["esquema"] == esquema),
            []
        )

        # Caso aninhamento = 0 → cria nó
        if grau_relacao == 0:
            for linha in linhas:
                nos.append({
                    "type": nome_tabela,
                    "scheme": esquema,
                    "isVertex": True,
                    "properties": linha
                })

        # Caso aninhamento = 1 → cria nó + relacionamento
        elif grau_relacao == 1 and len(chaves_estrangeiras) == 1:

            for linha in linhas:
                # Nó da tabela origem
                nos.append({
                    "type": nome_tabela,
                    "scheme": esquema,
                    "isVertex": True,
                    "properties": linha
                })

        # Caso grau_relacao > 2 → vários relacionamentos de cada FK para a tabela
        elif grau_relacao >= 2 and len(chaves_estrangeiras) >= 2 and len(colunas) > 2:
            for linha in linhas:

                nos.append({
                    "type": nome_tabela,
                    "scheme": esquema,
                    "isVertex": True,
                    "properties": linha
                })

    return nos