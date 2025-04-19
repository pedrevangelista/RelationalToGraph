def obterFks(inspector):
    json_relacionamentos = []
    # Obter todas as tabelas no banco de dados
    tabelas = inspector.get_table_names()

    # Iterar sobre as tabelas e obter informações sobre chaves estrangeiras (relacionamentos)
    for tabela in tabelas:
        foreign_keys = inspector.get_foreign_keys(tabela)
        
        # Verificar se a tabela tem chaves estrangeiras
        if foreign_keys:
            for fk in foreign_keys:
                json_fk = {
                    "nome_fk": fk['name'],
                    "nome_tabela": tabela,
                    "colunas_fk":  fk['constrained_columns'],
                    "tabela_referenciada": fk['referred_table'],
                    "colunas_referenciadas": fk['referred_columns']
                }
                json_relacionamentos.append(json_fk)
    return json_relacionamentos