def obterInformacoesRelacionamentos(json_tabelas):
    json_relacionamentos = []
    for tabela in json_tabelas:
        fks = tabela["chaves_estrangeiras"]
        for fk in fks:
            tabelas_referenciadas = obterTabelaReferenciadaRelacao(json_tabelas, tabela)
            grau_tabs_relacionadas = obterAninhamentoTabelas(tabelas_referenciadas)
            grau_relacionamento = tabela["aninhamento"] + grau_tabs_relacionadas
            json_fk = {
                "nome_fk": fk["nome_fk"],
                "coluna_fk": fk["coluna"],
                "target": fk["target"],
                "tabela_referenciada": fk["tabela_referenciada"],
                "coluna_referenciada": fk["coluna_referenciada"],
                "grau_relacionamento": grau_relacionamento,
            }
            json_relacionamentos.append(json_fk)
    return json_relacionamentos

        
def obterTabelaReferenciadaRelacao(tabelas, tabela_referencia):
    ref_tab = []
    for tabela in tabelas:
        if(tabela["nome_tabela"] == tabela_referencia["nome_tabela"] or len(tabela_referencia["chave_primaria"])==0):
            continue
        if(verificarChaveReferencia(tabela_referencia["chave_primaria"][0], tabela["chaves_estrangeiras"])):
            ref_tab.append(tabela)
    return ref_tab

def verificarChaveReferencia(chave_primaria, chaves_estrangeiras):
    if(len(chaves_estrangeiras) == 0):
        return  False
    for fk in chaves_estrangeiras:
        if fk["coluna_referenciada"] == chave_primaria:
            return True
    return False

def obterAninhamentoTabelas(tabelas):
    aninhamento_total = 0
    for tabela in tabelas:
        aninhamento_total = aninhamento_total + tabela["aninhamento"]
    return aninhamento_total
