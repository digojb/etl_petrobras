from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, MetaData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / "config" / ".env"
load_dotenv(env_path)

user = os.getenv('user')
password = os.getenv('senha')
database = os.getenv('database')
host = 'host.docker.internal'  # Usando host.docker.internal para acessar o banco de dados no host

def get_engine():
    logging.info(f"Conectado em {host}:5432/{database}")
    return create_engine(
    f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database}',
    connect_args={'client_encoding': 'utf8'})

engine = get_engine()
meta = MetaData()

def definir_e_criar_tabela(nome_tabela):
    tabela = Table(
        nome_tabela, 
        meta,

        Column('id', Integer, primary_key=True, autoincrement=True),
        
        Column('data_inicio', Date, nullable=False),
        Column('data_fim', Date, nullable=False),
        Column('combustivel', String, nullable=False),
        Column('preco_medio_brasil', Float), 
        Column('distribuicao', Float),  
        Column('porcentagem_distribuicao', Float),  
        Column('custo_etanol_anidro', Float),  
        Column('porcentagem_custo_etanol_anidro', Float),  
        Column('icms', Float),  
        Column('porcentagem_icms', Float),  
        Column('imposto_federal', Float),  
        Column('porcentagem_impostos_federais', Float),  
        Column('parcela_petrobras', Float),  
        Column('porcentagem_parcela_petrobras', Float),  
        Column('biodiesel', Float),  
        Column('porcentagem_biodiesel', Float),  
        
        UniqueConstraint('data_inicio', 'data_fim', 'combustivel', name=f'uix_{nome_tabela}_conflito'),
        
        extend_existing=True 
    )
    
    meta.create_all(engine)
    logging.info(f"Tabela '{nome_tabela}' verificada/criada com sucesso.")
    
    return tabela

def carregar_dados(nome_tabela, df):
    tabela = definir_e_criar_tabela(nome_tabela)

    dados = df.to_dict(orient='records')

    with engine.begin() as conn:
        stmt = insert(tabela)
        stmt = stmt.values(dados)

        stmt = stmt.on_conflict_do_nothing(
            index_elements=['data_inicio', 'data_fim', 'combustivel']
        )

        conn.execute(stmt)

    logging.info(f"{len(df)} registros processados (bulk insert).")

    df_check = pd.read_sql(f"SELECT COUNT(*) FROM {nome_tabela}", engine)
    logging.info(f"Total de registros na tabela {nome_tabela}: {df_check.iloc[0,0]}")