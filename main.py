from copy import deepcopy
import sys
import os
import time


sys.path.append(os.path.abspath("."))

from saida_grafo.ConexaoSaida import get_driver, import_data
from janus.GerarJanus import gerar_vertice_janus, gerar_edges_janus
from saida_grafo.GerarSaidaRelacionamentos import gerar_saida_relacionamentos
from saida_grafo.GerarSaidaNos import gerar_saida_de_nos
from cypher.GerarRelacionamentosCypher import  gerar_relacionamentos_cypher_dados
from cypher.GerarNosCypher import  gerar_nos_cypher_dados
from cypher.ExecutarCypher import executar_cypher
from grafo.GerarNosGenerico import  gerar_nos_generico_deprecated, gerar_nos_grafo
from grafo.GerarRelacionamentosGenerico import gerar_arestas_generico
from grafo.GerarGrafo import gerar_grafo
from relacional.GerarParametrosBanco import gerar_parametros_json, gerar_parametros_aninhados
from relacional.ConexaoBanco import obter_metadata_banco
from relacional.GerarEstruturaTabelaRelacional import  obter_estruturas_relacional, reescrever_metadados_com_aninhamento
from relacional.AninhamentoRelacoes import  aninhar_relacoes
from relacional.ChavesEstrangeiras import obterFks
from relacional.InformacoesRelacionamentos import obterInformacoesRelacionamentos
from saidas.GerarOutput import gerar_output, gerar_output_json

def main():

    #nome_banco = 'Alunos'
    #nome_banco = 'NORTHWND'
    nome_banco = 'AdventureWorks2022'
    #nome_banco = 'SchoolSample'
    #nome_banco = 'SOCCER_DB'
    #nome_banco = 'EsportsCSGO'
    #nome_banco = 'NORTHWND'
    database = 'NORTHWND'

    [metadata, conn] = obter_metadata_banco(nome_banco)
    
    estruturas_tabelas = obter_estruturas_relacional(metadata)
            
    gerar_output_json(nome_banco, estruturas_tabelas, "[1] METADADO - estruturas_tabelas_inicial.json")
    
    estruturas_tabelas = aninhar_relacoes(estruturas_tabelas)

    gerar_output_json(nome_banco, estruturas_tabelas, "[2] METADADO - estruturas_tabelas_pre_aninhar.json")

    parametros_tabela_aninhado = gerar_parametros_aninhados(estruturas_tabelas, conn)

    gerar_output_json(nome_banco, parametros_tabela_aninhado, "[3] VALORES - parametros_tabela_aninhado.json")

    novas_tabelas = reescrever_metadados_com_aninhamento(estruturas_tabelas)
    
    gerar_output_json(nome_banco, novas_tabelas, "[4] METADADO - estruturas_tabelas_aninhadas.json")
    
    grafo_generico = gerar_grafo(novas_tabelas, parametros_tabela_aninhado)

    nos_generico = grafo_generico["nodes"]
    arestas_generico = grafo_generico["relationships"]
    
    gerar_output_json(nome_banco, grafo_generico, "[5] GRAFO COMPLETO - grafo_generico.json")
    gerar_output_json(nome_banco, nos_generico, "[5] NOS - nos_generico.json")
    gerar_output_json(nome_banco, arestas_generico, "[5] ARESTAS - arestas_generico.json")

        
    vertices_janus_teste = gerar_vertice_janus(nos_generico)
    
    gerar_output_json(nome_banco, vertices_janus_teste, "[6] JANUS - vertices_janus_teste.json")

    edges_janus_teste = gerar_edges_janus(arestas_generico)
    
    gerar_output_json(nome_banco, edges_janus_teste, "[6] JANUS - edges_janus_teste.json")
    driver = get_driver()
    #import_data(driver, grafo_generico, database)
    driver.close()

    #no_cypher_teste = gerar_nos_cypher_dados(nos_generico)
    #relaciomentos_cypher = gerar_relacionamentos_cypher_dados(arestas_generico)

    #executar_cypher(no_cypher_teste)
    #executar_cypher(relaciomentos_cypher)
    
    conn.close()

    #gerar_saida_de_nos(cypher, valores_banco)

    #gerar_saida_relacionamentos(relaciomentos_cypher, valores_banco)



    # O JSON agora contém as informações sobre as tabelas e suas relações

    #json_formatted_str = json.dumps(json_relacionamentos, indent=4)

    #print(json_formatted_str)
    

main()
