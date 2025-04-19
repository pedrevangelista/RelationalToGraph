def gerar_nos_e_arestas(tabelas, valores_banco):
    nos = []
    arestas = []

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
                    "isVertex": True,
                    "properties": linha
                })

        # Caso aninhamento = 1 → cria nó + relacionamento
        elif grau_relacao == 1 and len(chaves_estrangeiras) == 1:
            fk = chaves_estrangeiras[0]
            tabela_destino = fk["tabela_referenciada"]

            for linha in linhas:
                # Nó da tabela origem
                nos.append({
                    "type": nome_tabela,
                    "isVertex": True,
                    "properties": linha
                })

                # Aresta ligando ao destino
                aresta = {
                    "type": nome_tabela + tabela_destino,
                    "isVertex": False,
                    "sourceLabel": nome_tabela,
                    "targetLabel": tabela_destino,
                    "source": fk["coluna"],
                    "target": fk["coluna_referenciada"],
                    "properties": {
                        fk["coluna"]: linha.get(fk["coluna"]),
                        fk["coluna_referenciada"]: linha.get(fk["coluna_referenciada"])
                    }
                }
                
                arestas.append(aresta)

        # Caso grau_relacao = 2 (relacionamento N:N)
        elif grau_relacao == 2 and len(chaves_estrangeiras) == 2 and len(colunas) == 2:
            fk_a, fk_b = chaves_estrangeiras
            tabela_a = fk_a["tabela_referenciada"]
            tabela_b = fk_b["tabela_referenciada"]
            for linha in linhas:
                aresta = {
                    "type": tabela_a + "_" + nome_tabela + "_" + tabela_b,
                    "isVertex": False,
                    "sourceLabel": tabela_a,
                    "targetLabel": tabela_b,
                    "source": linha.get(fk_a["coluna"]),
                    "target": linha.get(fk_b["coluna"]),
                    "properties": {
                        fk_a["coluna"]: linha.get(fk_a["coluna"]),
                        fk_b["coluna"]: linha.get(fk_b["coluna"])
                    }
                }
                arestas.append(aresta)

        # Caso grau_relacao > 2 → vários relacionamentos de cada FK para a tabela
        elif grau_relacao >= 2:
            for linha in linhas:

                nos.append({
                    "type": nome_tabela,
                    "isVertex": True,
                    "properties": linha
                })
                
                for fk in chaves_estrangeiras:
                    tabela_ref = fk["tabela_referenciada"]

                    arestas.append({
                        "type": nome_tabela + "_" + tabela_ref,
                        "isVertex": False,
                        "sourceLabel": tabela_ref,
                        "targetLabel": nome_tabela,
                        "source": fk["coluna"],
                        "target": fk["coluna"],
                        "properties": {
                            fk["coluna"]: linha.get(fk["coluna"])
                        }
                    })

    return nos, arestas