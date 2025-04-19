import re
from neo4j import GraphDatabase

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