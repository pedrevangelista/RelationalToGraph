import pyodbc
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import MetaData, Table, inspect

from utils.Geral import medir_tempo

def obter_metadata(nome_banco):
    # Conectar-se ao banco de dados SQL Server
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

    return [engine, conn]


def refletir_todos_os_schemas(engine, ignorar_sistemas=True):
    metadata = MetaData()
    inspector = inspect(engine)

    schemas_sistema = {'INFORMATION_SCHEMA', 'sys', 'guest'}
    schemas = inspector.get_schema_names()

    for schema in schemas:
        if ignorar_sistemas and schema in schemas_sistema:
            continue
        for table_name in inspector.get_table_names(schema=schema):
            Table(table_name, metadata, autoload_with=engine, schema=schema)

    return metadata

@medir_tempo
def obter_metadata_banco(nome_banco):
    engine, conn = obter_metadata(nome_banco)
    metadata = refletir_todos_os_schemas(engine)
    return metadata, conn
