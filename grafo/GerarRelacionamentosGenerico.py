import json
def gerar_arestas_generico(estruturas, parametros_aninhados, id_map):
    arestas = []
    for tabela in estruturas:
        # Caso 2a: Tabelas N:N (matriz de relacionamento)
        if tabela["tipo"] == "N:N":
            dados_tabela = next(
                (p for p in parametros_aninhados 
                 if p["tabela"] == tabela["nome_tabela"] and p["esquema"] == tabela["esquema"]),
                None
            )
            
            if dados_tabela and len(tabela["chaves_estrangeiras"]) >= 2:
                fk1, fk2 = tabela["chaves_estrangeiras"][:2]
                
                for item in dados_tabela["parametros"]:
                    # Obter IDs de origem e destino
                    from_id = id_map.get((
                        fk1["schema_referenciado"], 
                        fk1["tabela_referenciada"], 
                        item[fk1["coluna_local"]]
                    ))
                    
                    to_id = id_map.get((
                        fk2["schema_referenciado"], 
                        fk2["tabela_referenciada"], 
                        item[fk2["coluna_local"]]
                    ))
                    
                    if from_id and to_id:
                        # Criar propriedades extras (se houver)
                        props = {
                            k: v for k, v in item.items()
                            if k not in [fk1["coluna_local"], fk2["coluna_local"]]
                        }
                        
                        arestas.append({
                            "type": f"{fk1['tabela_referenciada']}_PARA_{fk2['tabela_referenciada']}",
                            "scheme": tabela["esquema"],
                            "isVertex": False,
                            "from": from_id,
                            "to": to_id,
                            "properties": props
                        })

        # Caso 2b: Relacionamentos 1:N (via FK)
        elif tabela["aninhamento"] == 0:
            for fk in tabela["chaves_estrangeiras"]:
                dados_tabela = next(
                    (p for p in parametros_aninhados 
                     if p["tabela"] == tabela["nome_tabela"] and p["esquema"] == tabela["esquema"]),
                    None
                )
                
                if dados_tabela:
                    for item in dados_tabela["parametros"]:
                        # Origem: Esta tabela (que contém a FK)
                        pk = tabela["chave_primaria"][0]
                        from_id = id_map.get((
                            tabela["esquema"],
                            tabela["nome_tabela"],
                            item[pk]
                        ))
                        
                        # Destino: Tabela referenciada
                        to_id = id_map.get((
                            fk["schema_referenciado"],
                            fk["tabela_referenciada"],
                            item[fk["coluna_local"]]
                        ))
                        
                        if from_id and to_id:
                            arestas.append({
                                "type": fk["nome_fk"],
                                "scheme": tabela["esquema"],
                                "isVertex": False,
                                "from": from_id,
                                "to": to_id,
                                "properties": {}
                            })
    return arestas

def gerar_arestas_generico_deprecated(tabelas, valores_banco):
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
                    "target": fk["coluna_local"],
                    "properties": {
                        fk["coluna_local"]: linha.get(fk["coluna_local"]),
                        fk["coluna_referenciada"]: linha.get(fk["coluna_local"])
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
                    "target": fk_b["coluna_local"],
                    "properties": {
                        fk_a["coluna_referenciada"]: linha.get(fk_a["coluna_local"]),
                        fk_b["coluna_referenciada"]: linha.get(fk_b["coluna_local"]),
                        fk_a["coluna_local"]: linha.get(fk_a["coluna_local"]),
                        fk_b["coluna_local"]: linha.get(fk_b["coluna_local"]),
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
                        "target": fk["coluna_local"],
                        "properties": {
                            fk["coluna_local"]: linha.get(fk["coluna_local"]),
                            fk["coluna_referenciada"]: linha.get(fk["coluna_local"])
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