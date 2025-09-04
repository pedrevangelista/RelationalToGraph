# RelationalToGraph

Ferramenta para conversão de bancos de dados relacionais (em SQL Server) para representações de grafos, com suporte à exportação para a aplicação do Neo4j e json no modelo JanusGraph.

---

## 🎯 Objetivo
Este projeto foi desenvolvido para automatizar a transformação de dados de modelos relacionais para modelos orientados a grafos

---

## ⚙️ Configuração

Antes de executar, é necessário configurar **três pontos principais**:

### 1. Nome dos bancos (Relacional e Grafo)
No arquivo `main.py`, informe:
- O **nome do banco relacional** (SQL Server) que será lido.
- O **nome do banco em grafo** que será exportado.

```python
# Exemplo dentro de main.py	
nome_banco_relacional = "AdventureWorks2022"
nome_banco_grafo = "GraphDB_AdventureWorks"
```

---

### 2. Conexão com o Banco de Grafos
No arquivo `saida_grafo/ConexaoSaida.py`, configure a URI, usuário e senha do banco de grafos (ex.: Neo4j):

```python
# Exemplo de configuração no ConexaoSaida.py
uri = "bolt://localhost:7687"
usuario = "neo4j"
senha = "sua_senha_aqui"
```

---

### 3. Conexão com o Banco Relacional (SQL Server)
No arquivo `relacional/ConexaoBanco.py`, ajuste os parâmetros de conexão conforme seu ambiente:

```python
import pyodbc
from sqlalchemy import create_engine

conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};'
    f'SERVER=DESKTOP-SMUTIVO\\MSSQLSERVER01;'
    f'DATABASE={nome_banco};'
    f'Trusted_Connection=yes;'
)

# Use sqlalchemy para introspecção do banco de dados
engine = create_engine(
    f"mssql+pyodbc://@DESKTOP-SMUTIVO\\MSSQLSERVER01/{nome_banco}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
```

> **Observação**:  
> - Ajuste `SERVER` de acordo com sua instância do SQL Server.  

---

## 🚀 Execução

Após configurar os arquivos acima, basta rodar o programa principal:

```bash
python main.py
```

Os resultados JSONs serão exportados para a pasta `./saidas/`. Além disso no Neo4j será possível visualizar o banco em grafos.

---

## 📦 Instalação
Clone o repositório e instale as dependências