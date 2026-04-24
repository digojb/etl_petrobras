# %%
from pathlib import Path

import pandas as pd
import json
import logging
from numpy import datetime64

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

arquivos = [
    (Path(__file__).resolve().parent.parent / 'data' / 'dados_gasolina.json', "gasolina"),
    (Path(__file__).resolve().parent.parent / 'data' / 'dados_diesel.json', "diesel"),
    (Path(__file__).resolve().parent.parent / 'data' / 'dados_glp.json', "glp"),
]

colunas_remover = [
    'Preço Médio Gasolina',
    'Preço Médio Diesel',
    'Período de coleta'
]

colunas_renomear = {
    'Preço Médio Brasil': 'preco_medio_brasil', 
    'Distribuição': 'distribuicao', 
    'Porcentagem Distribuição': 'porcentagem_distribuicao',
    'Custo do etanol anidro': 'custo_etanol_anidro',
    'Porcentagem Custo do etanol anidro': 'porcentagem_custo_etanol_anidro',
    'Imposto Estadual': 'icms',
    'Porcentagem Imposto Estadual': 'porcentagem_icms',
    'Imposto Federal': 'imposto_federal', 
    'Porcentagem Impostos Federais': 'porcentagem_impostos_federais',
    'Parcela Petrobras': 'parcela_petrobras', 
    'Porcentagem Parcela Petrobras': 'porcentagem_parcela_petrobras', 
    'Biodiesel': 'biodiesel',
    'Porcentagem Biodiesel': 'porcentagem_biodiesel',
    'Porcentagem ICMS': 'porcentagem_icms',
    'ICMS': 'icms',
}

def criar_dataframe(path_name) -> pd.DataFrame:
    path = path_name

    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.json_normalize(data)
    #print(df.columns)
    logging.info(f"DataFrame criado com sucesso a partir do arquivo: {path_name}")

    return df

def separar_data(df):
    df[["data_inicio", "data_fim"]] = df["Período de coleta"].str.split(" a ", expand = True)

    return df

def juntar_dados(dados) -> pd.DataFrame:
    dfs = []

    for tipo, data in dados.items():
        df = pd.json_normalize(data)
        df = separar_data(df)
        
        df["combustivel"] = tipo
        
        mapa_colunas = {
            "Imposto Estadual": "ICMS",
            "Porcentagem Imposto Estadual": "Porcentagem ICMS",
        }
        df.rename(columns=mapa_colunas, inplace=True)
        
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)
    logging.info("DataFrames juntados com sucesso.")

    return df_final

def remover_colunas(df, colunas_remover) -> pd.DataFrame:

    logging.info("Removendo colunas...")

    df.drop(columns=colunas_remover, inplace=True)

    logging.info("Colunas removidas com sucesso.")

    return df

def renomear_colunas(df, colunas_renomear) -> pd.DataFrame:

    logging.info("Renomeando colunas...")

    df.rename(columns=colunas_renomear, inplace=True)

    logging.info("Colunas renomeadas com sucesso.")

    return df

def transformar_float(valor):
    if pd.isna(valor):
        return None
    try:
        return float(str(valor).replace(",", "."))
    except:
        return None

def transformar_datetime(valor):
    if pd.isna(valor):
        return None
    try:
        return pd.to_datetime(valor, format="%d/%m/%Y")
    except:
        return None
      
def tratar_tipos(df) -> pd.DataFrame:
    df["data_inicio"] = df["data_inicio"].apply(transformar_datetime)
    df["data_fim"] = df["data_fim"].apply(transformar_datetime)

    for col in df.columns:
        if col not in ["data_inicio", "data_fim", "combustivel"]:
            df[col] = df[col].apply(transformar_float)
    
    logging.info("Tipos de dados tratados com sucesso.")

    return df

def validar_dados(df: pd.DataFrame):
    logging.info("Validando dados...")

    colunas_criticas = ["Preço Médio Brasil", "Período de coleta"]

    for col in colunas_criticas:
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {col}")

        if df[col].isnull().all():
            raise ValueError(f"Coluna {col} está totalmente nula")

    logging.info("Validação concluída com sucesso.")

def transformar_dados(dados):
    
    logging.info("Iniciando processo de transformação de dados...")

    df = juntar_dados(dados)
    validar_dados(df)
    df = remover_colunas(df, colunas_remover)
    df = tratar_tipos(df)
    df = renomear_colunas(df, colunas_renomear)
    
    logging.info("Processo de transformação de dados concluído com sucesso.")

    return df