def tipo_simplificado(col_type):
    from sqlalchemy import (
        Integer, String, Unicode, DateTime, Boolean, Float, Numeric
    )

    if isinstance(col_type, (String, Unicode)):
        return "string"
    elif isinstance(col_type, Integer):
        return "int"
    elif isinstance(col_type, Float):
        return "float"
    elif isinstance(col_type, Numeric):
        return "decimal"
    elif isinstance(col_type, Boolean):
        return "bool"
    elif isinstance(col_type, DateTime):
        return "datetime"
    else:
        return str(col_type.__class__.__name__)

def obter_estruturas_relacional(metadata):
    tabelas = metadata.tables.keys()
    json_tabelas=[]
    for tabela in tabelas:
        metadata_tabela = metadata.tables[tabela]
        partes = tabela.split('.')
        if len(partes) == 2:
            schema, nome_tabela = partes
        else:
            schema = 'dbo'  # ou outro padrão
            nome_tabela = partes[0]
        json_tabela = {
            "nome_tabela": nome_tabela,
            "esquema": schema,
            "chave_primaria": metadata_tabela.primary_key.columns.keys(),
            "colunas": [],
            "chaves_estrangeiras": []
        }

        # Adicione as colunas no JSON da tabela
        for coluna in metadata_tabela.columns:
            colunaFormatada = {
                "nome": coluna.name,
                "tipo": tipo_simplificado(coluna.type),
                "nullable": coluna.nullable
            }

            json_tabela["colunas"].append(colunaFormatada)

        # Obtenha as chaves estrangeiras da tabela
        chaves_estrangeiras = metadata_tabela.foreign_keys
        # Adicione as chaves estrangeiras para o JSON da tabela
        for chave_estrangeira in chaves_estrangeiras:
            json_fk = {
                "nome_fk": chave_estrangeira.name,
                "coluna": chave_estrangeira.parent.name,
                "target": chave_estrangeira.target_fullname, 
                "tabela_referenciada": chave_estrangeira.column.table.name,
                "schema_referenciado": chave_estrangeira.column.table.schema,
                "coluna_referenciada": chave_estrangeira.column.name
            }
            json_tabela["chaves_estrangeiras"].append(json_fk)
        
        json_tabelas.append(json_tabela)
        
    return json_tabelas


def reescrever_metadados_com_aninhamento(tabelas):
    from copy import deepcopy

    # Dicionário auxiliar para acesso e modificação
    tabelas_dict = {(t["esquema"], t["nome_tabela"]): deepcopy(t) for t in tabelas}

    # Primeira etapa: incorporar colunas das tabelas com aninhamento = 1 no pai
    for tabela in tabelas:
        if tabela["aninhamento"] == 1:
            fk = tabela["chaves_estrangeiras"][0]  # assume uma FK para o pai
            pai_key = (fk["schema_referenciado"], fk["tabela_referenciada"])
            tabela_pai = tabelas_dict[pai_key]

            # Adiciona colunas da filha ao pai (com prefixo)
            for col in tabela["colunas"]:
                if col["nome"] != fk["coluna"]:
                    nova_coluna = {
                        "nome": f"{tabela['nome_tabela']}{col['nome']}",
                        "tipo": col["tipo"],
                        "nullable": col["nullable"]
                    }
                    tabela_pai["colunas"].append(nova_coluna)

            # Adiciona a chave da filha também com prefixo
            # pk_col = next(c for c in tabela["colunas"] if c["nome"] == fk["coluna"])
            # nova_coluna_pk = {
            #     "nome": f"{tabela['nome_tabela']}{pk_col['nome']}",
            #     "tipo": pk_col["tipo"],
            #     "nullable": pk_col["nullable"]
            # }
            # tabela_pai["colunas"].append(nova_coluna_pk)

    # Segunda etapa: reconstruir as tabelas com FKs redirecionadas
    novas_tabelas = []

    for (esquema, nome), tabela in tabelas_dict.items():
        if tabela["aninhamento"] == 1:
            continue  # não incluímos as tabelas aninhadas no resultado

        nova_tabela = deepcopy(tabela)
        novas_fks = []

        for fk in tabela["chaves_estrangeiras"]:
            ref_key = (fk["schema_referenciado"], fk["tabela_referenciada"])
            tabela_ref = tabelas_dict.get(ref_key)
            if tabela_ref and tabela_ref["aninhamento"] == 1:
                # Redirecionar FK para o "avô" (pai da tabela aninhada)
                fk_aninhada = tabela_ref["chaves_estrangeiras"][0]
                novo_nome_coluna = f"{tabela_ref['nome_tabela']}{fk['coluna_referenciada']}"
                nova_fk = {
                    **fk,
                    "tabela_referenciada": fk_aninhada["tabela_referenciada"],
                    "schema_referenciado": fk_aninhada["schema_referenciado"],
                    "coluna_referenciada": novo_nome_coluna,
                    "target": f"{fk_aninhada['schema_referenciado']}.{fk_aninhada['tabela_referenciada']}.{novo_nome_coluna}"
                }
                novas_fks.append(nova_fk)
            else:
                novas_fks.append(fk)

        nova_tabela["chaves_estrangeiras"] = novas_fks
        novas_tabelas.append(nova_tabela)

    return novas_tabelas