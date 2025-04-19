from saida_grafo.ConexaoSaida import ajustar_placeholders_para_neo4j, executar_cypher_batch, executar_cypher_multi_params, get_driver
from utils.TratarTiposValores import tratar_decimais

def executar_cypher(comandos):
    print("Inicio execução cypher")
    driver = get_driver()

    batch_size = 100
    cyphers = []
    for comando in comandos:
        cyphers.append(comando["cypher"])

    with driver.session(database="geracaodatabase") as session:
        def run_batch(tx, batch):
            for cypher in batch:
                tx.run(cypher)
        for i in range(0, len(cyphers), batch_size):
            batch = cyphers[i:i + batch_size]
            session.execute_write(run_batch, batch)
        
    print("Fim execução cypher")
    driver.close()