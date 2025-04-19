def gerar_nos_generico(tabelas, valores_banco):
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