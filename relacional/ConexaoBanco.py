import pyodbc
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import MetaData, Table, inspect

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

def refletir_todos_os_schemas(engine):
    inspector = inspect(engine)
    metadata = MetaData()

    for schema in inspector.get_schema_names():
        if schema in ['INFORMATION_SCHEMA', 'sys', 'guest']:  # ignora schemas de sistema
            continue
        for table_name in inspector.get_table_names(schema=schema):
            Table(table_name, metadata, autoload_with=engine, schema=schema)

    return metadata

def obter_metadata_banco(nome_banco):
    [engine, conn] = obter_metadata(nome_banco)
    metadata = refletir_todos_os_schemas(engine)
    metadata.reflect(bind=engine, schema='dbo')
    return [metadata, conn]