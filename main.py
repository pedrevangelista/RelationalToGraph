import sys
import os

sys.path.append(os.path.abspath("."))

from saida_grafo.GerarSaidaRelacionamentos import gerar_saida_relacionamentos
from saida_grafo.GerarSaidaNos import gerar_saida_de_nos
from cypher.GerarRelacionamentosCypher import  gerar_relacionamentos_cypher_dados
from cypher.GerarNosCypher import  gerar_nos_cypher_dados
from cypher.ExecutarCypher import executar_cypher
from grafo.GerarNosGenerico import  gerar_nos_generico
from grafo.GerarRelacionamentosGenerico import gerar_arestas_generico
from grafo.GerarGenericoObsoleto import gerar_nos_e_arestas
from relacional.GerarParametrosBanco import gerar_parametros_json, gerar_parametros_aninhados
from relacional.ConexaoBanco import obter_metadata_banco
from relacional.GerarEstruturaTabelaRelacional import  obter_estruturas_relacional, reescrever_metadados_com_aninhamento
from relacional.AninhamentoRelacoes import  aninhar_relacoes
from relacional.ChavesEstrangeiras import obterFks
from relacional.InformacoesRelacionamentos import obterInformacoesRelacionamentos
from saidas.GerarOutput import gerar_output, gerar_output_json

def main():

    #nome_banco = 'Alunos'
    nome_banco = 'AdventureWorks2022'
    #nome_banco = 'SchoolSample'

    [metadata, conn] = obter_metadata_banco(nome_banco)
    
    json_tabelas = obter_estruturas_relacional(metadata)
    
    json_tabelas = aninhar_relacoes(json_tabelas)
            
    parametros_tabela_aninhado = gerar_parametros_aninhados(json_tabelas, conn)

    relacionamentos_graus = obterInformacoesRelacionamentos(json_tabelas)

    json_tabelas = reescrever_metadados_com_aninhamento(json_tabelas)
    
    nos_generico = gerar_nos_generico(json_tabelas, parametros_tabela_aninhado)

    arestas_generico = gerar_arestas_generico(json_tabelas, parametros_tabela_aninhado)

    

    no_cypher_teste = gerar_nos_cypher_dados(nos_generico)
    relaciomentos_cypher = gerar_relacionamentos_cypher_dados(arestas_generico)

    gerar_output_json(nome_banco, json_tabelas, "METADADO - json_tabelas.json")
    gerar_output_json(nome_banco, relacionamentos_graus, "METADADO - relacionamentos_graus.json")
    gerar_output_json(nome_banco, parametros_tabela_aninhado, "VALORES_BANCO_parametros_tabela_aninhado.json")
    gerar_output_json(nome_banco, nos_generico, "NOS_nos_generico.json")
    gerar_output_json(nome_banco, arestas_generico, "ARESTAS_arestas_generico.json")
    gerar_output_json(nome_banco, no_cypher_teste, "COMANDOS_no_cypher.json")
    gerar_output_json(nome_banco, relaciomentos_cypher, "COMANDOS_relaciomentos_cypher.json")

    executar_cypher(no_cypher_teste)
    executar_cypher(relaciomentos_cypher)
    
    conn.close()

    #gerar_saida_de_nos(cypher, valores_banco)

    #gerar_saida_relacionamentos(relaciomentos_cypher, valores_banco)



    # O JSON agora contém as informações sobre as tabelas e suas relações

    #json_formatted_str = json.dumps(json_relacionamentos, indent=4)

    #print(json_formatted_str)
    

main()
