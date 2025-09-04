# Função para ir ao banco e pegar os dados
from utils.Geral import formatar_coluna, medir_tempo

def get_valores_banco(nome_tabela, esquema, colunas, conn):
    cursor = conn.cursor()

    # Gerar a consulta SQL para pegar os valores da tabela

    colunas_selecionadas = ", ".join([f"[{coluna['nome']}]" for coluna in colunas])
    query = f"SELECT {colunas_selecionadas} FROM [{esquema}].[{nome_tabela}]"
    
    cursor.execute(query)
    resultados = cursor.fetchall()

    # Criar um dicionário para armazenar os parâmetros
    parametros = []
    for resultado in resultados:
        param = {}
        for idx, coluna in enumerate(colunas):
            param[formatar_coluna(coluna["nome"])] = resultado[idx]
        parametros.append(param)
    
    
    return parametros

@medir_tempo
def gerar_parametros_aninhados(tabelas, conn):
    # Primeiro, pegar os dados de todas as tabelas
    dados_por_tabela = {}
    lookup_tabelas = {}

    # Criar lookup para tabelas por nome
    for tabela in tabelas:
        chave = (tabela["esquema"], tabela["nome_tabela"])
        lookup_tabelas[chave] = tabela

    # Coletar dados do banco para todas as tabelas
    for tabela in tabelas:
        nome_tabela = tabela["nome_tabela"]
        esquema = tabela["esquema"]
        colunas = tabela["colunas"]
        parametros = get_valores_banco(nome_tabela, esquema, colunas, conn)
        dados_por_tabela[(esquema, nome_tabela)] = {
            "dados": parametros,
            "tabela": tabela
        }

    # Construir o aninhamento
    # Passo 1: Identificar tabelas para aninhar (aninhamento == 1)
    tabelas_para_aninhar = [
        (key, info) for key, info in dados_por_tabela.items() 
        if info["tabela"].get("aninhamento") == 1
    ]

    # Passo 2: Para cada tabela aninhável, encontrar a tabela que a referencia
    for (esquema_ref, tabela_ref), info_ref in tabelas_para_aninhar:
        # Encontrar FK que referencia esta tabela
        tabela_pai = None
        fk_info = None
        
        for (esquema, tabela), info in dados_por_tabela.items():
            if (esquema, tabela) == (esquema_ref, tabela_ref):
                continue
                
            for fk in info["tabela"].get("chaves_estrangeiras", []):
                if (fk["schema_referenciado"] == esquema_ref and 
                    fk["tabela_referenciada"] == tabela_ref):
                    tabela_pai = (esquema, tabela)
                    fk_info = fk
                    break
            if tabela_pai:
                break

        if not tabela_pai or not fk_info:
            continue

        # Passo 3: Criar lookup para os dados da tabela aninhável
        pk_ref = info_ref["tabela"]["chave_primaria"][0]  # PK simples
        lookup_dados = {
            str(item[pk_ref]): item 
            for item in info_ref["dados"]
        }
        # Passo 4: Incorporar dados na tabela pai
        for item_pai in dados_por_tabela[tabela_pai]["dados"]:
            fk_value = item_pai.get(fk_info["coluna_local"])
            if fk_value is not None and str(fk_value) in lookup_dados:
                registro_aninhado = lookup_dados[str(fk_value)]
                
                # Adicionar cada campo do registro aninhado, exceto a PK
                for campo, valor in registro_aninhado.items():
                    if campo == pk_ref:  # Pular a PK (já está no pai como FK)
                        continue
                    # Criar o nome do campo aninhado: {tabela_ref}_{campo}
                    novo_campo = f"{tabela_ref}_{campo}"
                    item_pai[novo_campo] = valor

    # Passo 5: Remover tabelas aninhadas dos resultados
    chaves_aninhadas = {key for key, _ in tabelas_para_aninhar}
    
    # Construir lista final de resultados
    parametros_por_tabela = []
    for (esquema, nome_tabela), info in dados_por_tabela.items():
        if (esquema, nome_tabela) not in chaves_aninhadas:
            parametros_por_tabela.append({
                "tabela": nome_tabela,
                "esquema": esquema,
                "parametros": info["dados"]
            })

    return parametros_por_tabela
