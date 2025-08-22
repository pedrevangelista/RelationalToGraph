from copy import deepcopy

from utils.Geral import medir_tempo

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
    
@medir_tempo
def obter_estruturas_relacional(metadata):
    estruturas_tabelas = []

    for nome_completo, tabela in metadata.tables.items():
        
        pk_columns = {col.name for col in tabela.primary_key.columns}
        # Coletar restrições únicas (UNIQUE) não-PK
        colunas_unicas = set()
        for index in tabela.indexes:
            if index.unique:
                # Verificar se não é a PK
                index_columns = {col.name for col in index.columns}
                if index_columns != pk_columns:
                    colunas_unicas.add(tuple(sorted(col.name for col in index.columns)))

        estrutura = {
            "nome_tabela": tabela.name,
            "esquema": tabela.schema or "dbo",
            "chave_primaria": [col.name for col in tabela.primary_key.columns],
            "colunas": [],
            "chaves_estrangeiras": [],
            "colunas_unicas": list(colunas_unicas) 
        }

        for coluna in tabela.columns:
            estrutura["colunas"].append({
                "nome": coluna.name,
                "tipo": tipo_simplificado(coluna.type),
                "nullable": coluna.nullable
            })

        if(len(estrutura["chave_primaria"]) == 0):
            print(estrutura["colunas"][0])
            estrutura["chave_primaria"] = [estrutura["colunas"][0]["nome"]]

        for fk_constraint in tabela.foreign_key_constraints:
            # estrutura["chaves_estrangeiras"].append({
            #     "nome_fk": fk.name or f"fk_{fk.parent.name}_{fk.column.table.name}",
            #     "coluna": fk.parent.name,
            #     "target": fk.target_fullname,
            #     "tabela_referenciada": fk.column.table.name,
            #     "schema_referenciado": fk.column.table.schema or "dbo",
            #     "coluna_referenciada": fk.column.name
            # })

            colunas_locais = [col.name for col in fk_constraint.columns]
            colunas_ref = [col.column.name for col in fk_constraint.elements]
            tabela_ref = fk_constraint.elements[0].column.table.name
            schema_ref = fk_constraint.elements[0].column.table.schema or "dbo"

            # Verificar se é 1:1 (FK é única ou PK)
            fk_tuple = tuple(sorted(colunas_locais))
            is_unique = (
                fk_tuple in estrutura["colunas_unicas"] or 
                set(colunas_locais) == set(estrutura["chave_primaria"])
            )
            cardinalidade = "1:1" if is_unique else "1:N"

            estrutura["chaves_estrangeiras"].append({
                "nome_fk": fk_constraint.name,
                "coluna_local": colunas_locais[0],
                "tabela_referenciada": tabela_ref,
                "schema_referenciado": schema_ref,
                "coluna_referenciada": colunas_ref[0],
                "cardinalidade": cardinalidade  # Adiciona a cardinalidade
            })

                # Identificar relações N:N (versão adaptada para tabelas sem PK explícita)
        fks = estrutura["chaves_estrangeiras"]
        if len(fks) >= 2:
            # Se não tem PK definida mas todas colunas são FKs
            if not estrutura["chave_primaria"]:
                all_columns = set(col["nome"] for col in estrutura["colunas"])
                fk_columns = set()
                for fk in fks:
                    fk_columns.update([fk["coluna_local"]])
                
                if all_columns == fk_columns:
                    estrutura["tipo"] = "N:N"
                    # Define PK virtual como todas colunas
                    estrutura["chave_primaria"] = list(all_columns)
                else:
                    estrutura["tipo"] = "Tabela Normal"
            else:
                # Lógica original para tabelas com PK
                pk_set = set(estrutura["chave_primaria"])
                fk_columns = set()
                for fk in fks:
                    fk_columns.update([fk["coluna_local"]])
                
                if pk_set == fk_columns:
                    estrutura["tipo"] = "N:N"
                else:
                    estrutura["tipo"] = "Tabela Normal"
        else:
            estrutura["tipo"] = "Tabela Normal"

        estruturas_tabelas.append(estrutura)

    return estruturas_tabelas


@medir_tempo
def reescrever_metadados_com_aninhamento(tabelas):
    from copy import deepcopy

    # Criar dicionário para acesso rápido às tabelas
    tabelas_dict = {(t["esquema"], t["nome_tabela"]): deepcopy(t) for t in tabelas}
    
    # Identificar tabelas para aninhar (aninhamento == 1)
    tabelas_aninhadas = []
    
    for chave, tabela in tabelas_dict.items():
        if tabela.get("aninhamento") == 1:
            # Encontrar tabela que referencia esta tabela (pai)
            tabela_pai = None
            
            for chave_pai, tabela_candidata in tabelas_dict.items():
                if chave_pai == chave:
                    continue
                    
                for fk in tabela_candidata.get("chaves_estrangeiras", []):
                    if (fk["schema_referenciado"] == tabela["esquema"] and 
                        fk["tabela_referenciada"] == tabela["nome_tabela"]):
                        tabela_pai = tabela_candidata
                        coluna_fk = fk["coluna_local"]
                        break
                if tabela_pai:
                    break
                    
            if not tabela_pai:
                continue

            # Adicionar colunas da tabela aninhada ao pai (prefixadas)
            pk_aninhada = tabela["chave_primaria"][0]  # Assumindo PK simples
            
            for col in tabela["colunas"]:
                # Não adicionar a coluna PK (já existe no pai como FK)
                if col["nome"] == pk_aninhada:
                    continue
                    
                nova_coluna = deepcopy(col)
                nova_coluna["nome"] = f"{tabela['nome_tabela']}_{col['nome']}"
                tabela_pai["colunas"].append(nova_coluna)
                
            # Remover a FK do pai que referenciava a tabela aninhada
            tabela_pai["chaves_estrangeiras"] = [
                fk for fk in tabela_pai.get("chaves_estrangeiras", [])
                if not (fk["schema_referenciado"] == tabela["esquema"] and 
                        fk["tabela_referenciada"] == tabela["nome_tabela"])
            ]
            
            tabelas_aninhadas.append(chave)

    # Remover tabelas aninhadas dos resultados
    for chave in tabelas_aninhadas:
        if chave in tabelas_dict:
            del tabelas_dict[chave]
            
    # Atualizar campos calculados
    for tabela in tabelas_dict.values():
        tabela["qtd_fks"] = len(tabela.get("chaves_estrangeiras", []))

        if tabela["tipo"] == "N:N" and tabela["qtd_fks"] < 2:
            tabela["tipo"] = "Tabela Normal"
        
        # O grau de relacionamento precisa ser recalculado
        tabela["grau_relacao"] = 0
        for outra_tabela in tabelas_dict.values():
            if outra_tabela == tabela:
                continue
                
            for fk in outra_tabela.get("chaves_estrangeiras", []):
                if (fk["schema_referenciado"] == tabela["esquema"] and 
                    fk["tabela_referenciada"] == tabela["nome_tabela"]):
                    tabela["grau_relacao"] += 1
                    break  # Conta apenas uma referência por tabela

    return list(tabelas_dict.values())