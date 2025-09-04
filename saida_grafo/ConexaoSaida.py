import re
from neo4j import GraphDatabase
from utils.Geral import medir_tempo

# URL do seu Neo4j (pode ser localhost se você tiver rodando localmente)
uri = "bolt://localhost:7687"  # ou "neo4j://localhost:7687" dependendo da sua configuração
usuario = "teste"  # Seu nome de usuário
senha = "senha7355608"  # Sua senha

# NEO4J_URI="neo4j+s://592b9e03.databases.neo4j.io"
# NEO4J_USERNAME="neo4j"
# NEO4J_PASSWORD="zzTuQS8KVfynpCYoHUOqU0EMxxFXcbHC-yIosrF9Jl0"
# AURA_INSTANCEID="592b9e03"
# AURA_INSTANCENAME="Instance01"

def get_driver():
    return GraphDatabase.driver(uri, auth=(usuario, senha))

# Função para executar o Cypher
def executar_cypher(driver, cypher, parametros):
    with driver.session(database = "geracaodatabase") as session:
        session.run(cypher, parametros)
        print("Cypher executado com sucesso.")


def executar_cypher_em_lote(driver, cypher_list, parametros_list):
    # Criando uma instância do driver de conexão
    with driver.session(database = "geracaodatabase") as session:
        for cypher, parametros in zip(cypher_list, parametros_list):
            session.run(cypher, parametros)
        print("Todos os Cyphers executados com sucesso.")

def executar_cypher_multi_params(driver, cypher, parametros_list):
    # Criando uma instância do driver de conexão
    with driver.session(database = "geracaodatabase") as session:
        for param in parametros_list:
            session.run(cypher, param)
def executar_cypher_batch(driver, cypher, parametros_list):
    
    with driver.session(database = "geracaodatabase") as session:
                        session.run(cypher, {"lote": parametros_list})

def ajustar_placeholders_para_neo4j(cypher_com_template):
    return re.sub(r"\$\{(\w+)\}", r"$\1", cypher_com_template)

def import_data(driver, grafo_data, database):
    with driver.session(database = database) as session:
        # Criar índice
        session.execute_write(create_indexes)
        # Criar nós em lotes
        session.execute_write(create_nodes_batch, grafo_data["nodes"], batch_size=5000)
        # Importar relacionamentos em lotes
        session.execute_write(create_relationships_batch, grafo_data["relationships"], batch_size=5000)


def create_indexes(tx):
    """Cria índices necessários para melhorar performance"""
    queries = [
        "CREATE INDEX node_id_index IF NOT EXISTS FOR (n:Node) ON (n.id)",
        # Adicione outros índices específicos se necessário
    ]
    
    for query in queries:
        try:
            tx.run(query)
            print(f"Índice criado: {query}")
        except Exception as e:
            print(f"Erro ao criar índice: {e}")

@medir_tempo
def create_nodes_batch(tx, nodes, batch_size):
    # Agrupar nós por tipo
    nodes_by_type = {}
    for node in nodes:
        node_type = node["type"]
        if node_type not in nodes_by_type:
            nodes_by_type[node_type] = []
        nodes_by_type[node_type].append(node)
    
    # Processar cada tipo em lotes
    for node_type, node_list in nodes_by_type.items():
        safe_type = f"`{node_type}`" if " " in node_type else node_type
        # Dividir em lotes menores
        for i in range(0, len(node_list), batch_size):
            batch = node_list[i:i+batch_size]
            query = f"""
            UNWIND $nodes AS node
            MERGE (n:Node:{safe_type} {{id: node.id}})
            SET n += node.properties
            """
            
            try:
                tx.run(query, nodes=batch)
            except Exception as e:
                print(f"Erro no lote {i//batch_size} de {node_type}: {e}")
                # Tentar novamente com lote menor
                for single_node in batch:
                    try:
                        tx.run(query, nodes=[single_node])
                    except:
                        print(f"Falha persistente no nó: {single_node['id']}")

@medir_tempo
def create_relationships_batch(tx, relationships, batch_size):
    # Agrupar relacionamentos por tipo
    rels_by_type = {}
    for rel in relationships:
        rel_type = rel["type"]
        if rel_type not in rels_by_type:
            rels_by_type[rel_type] = []
        rels_by_type[rel_type].append(rel)
    
    # Processar cada tipo em lotes
    for rel_type, rel_list in rels_by_type.items():
        safe_rel_type = f"`{rel_type}`" if " " in rel_type else rel_type
        # Dividir em lotes menores
        for i in range(0, len(rel_list), batch_size):
            batch = rel_list[i:i+batch_size]
            
            query = f"""
            UNWIND $rels AS rel
            MATCH (from:Node {{id: rel.from}}), (to:Node {{id: rel.to}})
            MERGE (from)-[r:{safe_rel_type}]->(to)
            SET r += rel.properties
            """
            
            tx.run(query, rels=batch)