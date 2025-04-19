# Função para ir ao banco e pegar os dados
from utils.Geral import formatar_coluna
from collections import defaultdict

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

# Função para gerar os parâmetros JSON
def gerar_parametros_json(tabelas, conn):
    parametros_por_tabela = []

    for tabela in tabelas:
        nome_tabela = tabela["nome_tabela"]
        esquema = tabela["esquema"]
        colunas = tabela["colunas"]
        
        # Obter os valores reais do banco para essa tabela
        parametros = get_valores_banco(nome_tabela, esquema, colunas, conn)
        
        parametros_por_tabela.append({
            "tabela": nome_tabela,
            "esquema": esquema,
            "parametros": parametros
        })
    return parametros_por_tabela


def gerar_parametros_aninhados(tabelas, conn):
    # Primeiro, pegar os dados de todas as tabelas
    dados_por_tabela = {}

    for tabela in tabelas:
        nome_tabela = tabela["nome_tabela"]
        esquema = tabela["esquema"]
        colunas = tabela["colunas"]
        parametros = get_valores_banco(nome_tabela, esquema, colunas, conn)
        dados_por_tabela[(esquema, nome_tabela)] = {
            "dados": parametros,
            "tabela": tabela
        }

    # Agora, tratar o aninhamento
    for (esquema_filha, nome_filha), info_filha in dados_por_tabela.items():
        tabela_filha = info_filha["tabela"]
        if tabela_filha.get("aninhamento") != 1:
            continue

        for fk in tabela_filha.get("chaves_estrangeiras", []):
            esquema_pai = fk["schema_referenciado"]
            nome_pai = fk["tabela_referenciada"]
            coluna_filha = fk["coluna"]
            coluna_pai = fk["coluna_referenciada"]

            dados_pai = dados_por_tabela.get((esquema_pai, nome_pai))
            if not dados_pai:
                continue

            # Agrupar as linhas da filha por FK
            dados_filha = info_filha["dados"]
            dados_agrupados = defaultdict(list)
            for linha in dados_filha:
                chave = linha[coluna_filha]
                dados_agrupados[chave].append(linha)

            novas_linhas_pai = []
            for linha_pai in dados_pai["dados"]:
                valor_chave = linha_pai.get(coluna_pai)
                linhas_filhas = dados_agrupados.get(valor_chave, [])

                if linhas_filhas:
                    for filha in linhas_filhas:
                        nova_linha = linha_pai.copy()
                        for chave, valor in filha.items():
                            if chave != coluna_filha:
                                nova_coluna = tabela_filha["nome_tabela"] + chave
                                #nova_coluna = chave
                                nova_linha[nova_coluna] = valor
                        novas_linhas_pai.append(nova_linha)
                else:
                    novas_linhas_pai.append(linha_pai)

            # Substitui os dados da tabela pai pelas novas linhas duplicadas
            dados_pai["dados"] = novas_linhas_pai

    # Agora construir a lista de retorno ignorando tabelas com aninhamento == 1
    parametros_por_tabela = []
    for (esquema, nome), info in dados_por_tabela.items():
        if info["tabela"].get("aninhamento") != 1:
            parametros_por_tabela.append({
                "tabela": nome,
                "esquema": esquema,
                "parametros": info["dados"]
            })

    return parametros_por_tabela
