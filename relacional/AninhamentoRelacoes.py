import copy

from utils.Geral import medir_tempo

"""
    Marca tabelas com aninhamento = 1 quando atendem critérios estruturais:
    - Chave primária simples
    - Poucos dados reais (máx. 2 colunas que não são PK/FK)
    - Referenciada no máximo 1 vez
    """
@medir_tempo
def aninhar_relacoes(estruturas_tabelas, max_colunas=2):
    nova_lista = []
    for tabela in estruturas_tabelas:
        nova_tabela = copy.deepcopy(tabela)
        referencias_tabela = quantidade_referencias_tabela(estruturas_tabelas, tabela)

        criterios = [
            chave_primaria_unica(tabela),
            tabela_tem_poucos_dados(tabela, max_colunas),
            referencias_tabela == 1
        ]

        nova_tabela["aninhamento"] = 1 if all(criterios) else 0
        nova_tabela["qtd_fks"] = len(tabela.get("chaves_estrangeiras", []))
        nova_tabela["grau_relacao"] = referencias_tabela

        nova_lista.append(nova_tabela)

    # Ordenar tabelas por "grau" de dependência
    return sorted(nova_lista, key=lambda x: x["grau_relacao"])

def tabela_tem_poucos_dados(tabela, max_colunas=2):
    pk = set(tabela.get("chave_primaria", []))

    colunas_reais = [
        col for col in tabela.get("colunas", [])
        if col["nome"] not in pk
    ]
    return len(colunas_reais) < max_colunas

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
