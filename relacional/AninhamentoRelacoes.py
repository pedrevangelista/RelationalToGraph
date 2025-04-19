def aninhar_relacoes(json_tabelas):
    nova_lista = []
    for tabela in json_tabelas:
        novo_json = tabela.copy()
        if (
            chave_primaria_unica(tabela)
            and tabela_tem_poucos_dados(tabela)
            and quantidade_referencias_tabela(json_tabelas, tabela) <= 1
            and apenas_uma_fk(tabela)
        ):
            novo_json["aninhamento"] = 1
        else:
            novo_json["aninhamento"] = 0

        novo_json["grau_relacao"] = len(tabela.get("chaves_estrangeiras", []))
        nova_lista.append(novo_json)
    return nova_lista

def tabela_tem_poucos_dados(tabela, max_colunas=2):
    pk = set(tabela.get("chave_primaria", []))
    fks = set(fk["coluna"] for fk in tabela.get("chaves_estrangeiras", []))

    colunas_reais = [
        col for col in tabela.get("colunas", [])
        if col["nome"] not in pk and col["nome"] not in fks
    ]
    return len(colunas_reais) <= max_colunas

def quantidade_referencias_tabela(tabelas, tabela_referencia):
    nome_ref = tabela_referencia["nome_tabela"]
    schema_ref = tabela_referencia["esquema"]
    colunas_ref = tabela_referencia.get("chave_primaria", [])

    if not colunas_ref:
        return 0

    total = 0
    for tabela in tabelas:
        if tabela["nome_tabela"] == nome_ref and tabela["esquema"] == schema_ref:
            continue

        for fk in tabela.get("chaves_estrangeiras", []):
            if (
                fk["tabela_referenciada"] == nome_ref
                and fk["schema_referenciado"] == schema_ref
                and fk["coluna_referenciada"] in colunas_ref
            ):
                total += 1
                break  # Só conta uma vez por tabela

    return total

def chave_primaria_unica(tabela):
    return len(tabela.get("chave_primaria", [])) == 1

def apenas_uma_fk(tabela):
    return len(tabela.get("chaves_estrangeiras", [])) == 1

