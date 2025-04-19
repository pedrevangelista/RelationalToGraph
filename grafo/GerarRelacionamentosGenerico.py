import json


def gerar_arestas_generico(tabelas, valores_banco):
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

        if grau_relacao == 1 and len(chaves_estrangeiras) == 1:
            fk = chaves_estrangeiras[0]
            tabela_destino = fk["tabela_referenciada"]
            for linha in linhas:
                # Aresta ligando ao destino
                aresta = {
                    "type": nome_tabela + tabela_destino,
                    "isVertex": False,
                    "sourceLabel": nome_tabela,
                    "targetLabel": tabela_destino,
                    "source": fk["coluna_referenciada"],
                    "target": fk["coluna"],
                    "properties": {
                        fk["coluna"]: linha.get(fk["coluna"]),
                        fk["coluna_referenciada"]: linha.get(fk["coluna"])
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
                    "source": fk_a["coluna_referenciada"],
                    "target": fk_b["coluna"],
                    "properties": {
                        fk_a["coluna_referenciada"]: linha.get(fk_a["coluna"]),
                        fk_b["coluna_referenciada"]: linha.get(fk_b["coluna"]),
                        fk_a["coluna"]: linha.get(fk_a["coluna"]),
                        fk_b["coluna"]: linha.get(fk_b["coluna"]),
                    }
                }
                arestas.append(aresta)

        # Caso grau_relacao > 2 → vários relacionamentos de cada FK para a tabela
        elif grau_relacao >= 2:
            for linha in linhas:
                for fk in chaves_estrangeiras:
                    tabela_ref = fk["tabela_referenciada"]
                    arestas.append({
                        "type": nome_tabela + "_" + tabela_ref,
                        "isVertex": False,
                        "sourceLabel": tabela_ref,
                        "targetLabel": nome_tabela,
                        "source": fk["coluna_referenciada"],
                        "target": fk["coluna"],
                        "properties": {
                            fk["coluna"]: linha.get(fk["coluna"]),
                            fk["coluna_referenciada"]: linha.get(fk["coluna"])
                        }
                    })

    return remover_arestas_duplicadas(arestas)

def limpar_bytes(obj):
    if isinstance(obj, dict):
        return {k: limpar_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [limpar_bytes(v) for v in obj]
    elif isinstance(obj, bytes):
        try:
            return obj.decode('utf-8')  # tenta decodificar
        except Exception:
            return str(obj)  # se falhar, converte direto pra string
    else:
        return obj

def remover_arestas_duplicadas(arestas):
    print(arestas[0])
    arestas_unicas = []
    vistos = set()

    for aresta in arestas:
        # Serializa de forma consistente (ordenando as chaves)
        aresta_limpa = limpar_bytes(aresta)
        chave = json.dumps(aresta_limpa, sort_keys=True)

        if chave not in vistos:
            vistos.add(chave)
            arestas_unicas.append(aresta)

    return arestas_unicas