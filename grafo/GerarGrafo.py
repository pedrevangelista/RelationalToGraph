import decimal
from datetime import date, datetime

from utils.Geral import medir_tempo

def tratar_valor(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.hex()
    if v is None:
        return "None"
    if isinstance(v, str):
        return v.replace("'", "\\'")
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v

@medir_tempo
def gerar_grafo(estruturas, parametros_aninhados):
    nos = []
    relacionamentos = []
    id_map = {}  # Para mapear nós

    # 1. Criar nós apenas para tabelas que NÃO são N:N
    for tabela in estruturas:
        if tabela["aninhamento"] == 0 and tabela["tipo"] != "N:N":
            dados_tabela = next(
                (p for p in parametros_aninhados 
                 if p["tabela"] == tabela["nome_tabela"] and p["esquema"] == tabela["esquema"]),
                None
            )
            
            if dados_tabela:
                for item in dados_tabela["parametros"]:
                    converted_props = {k: tratar_valor(v) for k, v in item.items()}

                    pk = tabela["chave_primaria"][0]
                    node_id = f"{tabela['nome_tabela']}:{converted_props[pk]}"
                    
                    nos.append({
                        "id": node_id,
                        "type": tabela["nome_tabela"],
                        "scheme": tabela["esquema"],
                        "properties": converted_props
                    })
                    
                    id_map[(tabela["esquema"], tabela["nome_tabela"], converted_props[pk])] = node_id

    # 2. Criar relacionamentos
    for tabela in estruturas:
        if tabela["aninhamento"] == 0:
            dados_tabela = next(
                (p for p in parametros_aninhados 
                 if p["tabela"] == tabela["nome_tabela"] and p["esquema"] == tabela["esquema"]),
                None
            )
            
            if not dados_tabela:
                continue
                
            # Caso especial: Tabelas N:N
            if tabela["tipo"] == "N:N" and len(tabela["chaves_estrangeiras"]) >= 2:
                fk1, fk2 = tabela["chaves_estrangeiras"][:2]

                for item in dados_tabela["parametros"]:
                    converted_item = {k: tratar_valor(v) for k, v in item.items()}
                    origem = id_map.get((
                        fk1["schema_referenciado"], 
                        fk1["tabela_referenciada"], 
                        converted_item[fk1["coluna_local"]]
                    ))
                    
                    destino = id_map.get((
                        fk2["schema_referenciado"], 
                        fk2["tabela_referenciada"], 
                        converted_item[fk2["coluna_local"]]
                    ))
                    
                    if origem and destino:
                        relacionamentos.append({
                            "type": f"{fk1['tabela_referenciada']}_TO_{fk2['tabela_referenciada']}",
                            "scheme": tabela["esquema"],
                            "from": origem,
                            "to": destino,
                            "properties": {k: v for k, v in converted_item.items() if k not in [fk1["coluna_local"], fk2["coluna_local"]]}
                        })
                        relacionamentos.append({
                            "type": f"{fk2['tabela_referenciada']}_TO_{fk1['tabela_referenciada']}",
                            "scheme": tabela["esquema"],
                            "from": destino,
                            "to": origem,
                            "properties": {k: v for k, v in converted_item.items() if k not in [fk1["coluna_local"], fk2["coluna_local"]]}
                        })
            
            # Caso normal: Relacionamentos via FK
            else:
                for fk in tabela.get("chaves_estrangeiras", []):
                    for item in dados_tabela["parametros"]:
                        origem = id_map.get((
                            tabela["esquema"],
                            tabela["nome_tabela"],
                            item[tabela["chave_primaria"][0]]
                        ))
                        
                        destino = id_map.get((
                            fk["schema_referenciado"],
                            fk["tabela_referenciada"],
                            item[fk["coluna_local"]]
                        ))
                        
                        if origem and destino:
                            relacionamentos.append({
                                "type": f"{tabela['nome_tabela']}_TO_{fk['tabela_referenciada']}",
                                "scheme": tabela["esquema"],
                                "from": origem,
                                "to": destino,
                                "properties": {}
                            })

    return {
        "nodes": nos,
        "relationships": relacionamentos
    }