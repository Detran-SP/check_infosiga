import zipfile
import tempfile
import os
import pandas as pd
from pathlib import Path
from typing import Literal


def read_infosiga(path: str, file: Literal["pessoas", "veiculos", "sinistros"]) -> pd.DataFrame:
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        
        pattern = file
        path_file = None
        for f in os.listdir(temp_dir):
            if pattern in f.lower():
                path_file = os.path.join(temp_dir, f)
                break
        
        if path_file is None:
            raise FileNotFoundError(f"Arquivo '{file}' não encontrado no ZIP")
        
        if file == "sinistros":
            dtype_dict = {
                "id_sinistro": "Int64",
                "tipo_registro": "string",
                "ano_sinistro": "Int64",
                "mes_sinistro": "Int64",
                "dia_sinistro": "Int64",
                "ano_mes_sinistro": "string",
                "dia_da_semana": "string",
                "turno": "string",
                "logradouro": "string",
                "numero_logradouro": "Float64",
                "tipo_via": "string",
                "tipo_local": "string",
                "latitude": "Float64",
                "longitude": "Float64",
                "cod_ibge": "Int64",
                "municipio": "string",
                "regiao_administrativa": "string",
                "administracao": "string",
                "conservacao": "string",
                "circunscricao": "string",
                "tp_sinistro_primario": "string",
                "qtd_pedestre": "Int64",
                "qtd_bicicleta": "Int64",
                "qtd_motocicleta": "Int64",
                "qtd_automovel": "Int64",
                "qtd_onibus": "Int64",
                "qtd_caminhao": "Int64",
                "qtd_veic_outros": "Int64",
                "qtd_veic_nao_disponivel": "Int64",
                "qtd_gravidade_fatal": "Int64",
                "qtd_gravidade_grave": "Int64",
                "qtd_gravidade_leve": "Int64",
                "qtd_gravidade_ileso": "Int64",
                "qtd_gravidade_nao_disponivel": "Int64",
                "tp_sinistro_atropelamento": "string",
                "tp_sinistro_colisao_frontal": "string",
                "tp_sinistro_colisao_traseira": "string",
                "tp_sinistro_colisao_lateral": "string",
                "tp_sinistro_colisao_transversal": "string",
                "tp_sinistro_colisao_outros": "string",
                "tp_sinistro_choque": "string",
                "tp_sinistro_capotamento": "string",
                "tp_sinistro_engavetamento": "string",
                "tp_sinistro_tombamento": "string",
                "tp_sinistro_outros": "string",
                "tp_sinistro_nao_disponivel": "string",
            }
            date_columns = ["data_sinistro"]
            time_columns = ["hora_sinistro"]
        elif file == "pessoas":
            dtype_dict = {
                "id_sinistro": "Int64",
                "id_veiculo": "Int64",
                "cod_ibge": "Int64",
                "municipio": "string",
                "regiao_administrativa": "string",
                "tipo_via": "string",
                "tipo_veiculo_vitima": "string",
                "sexo": "string",
                "idade": "Int64",
                "gravidade_lesao": "string",
                "tipo_de_vitima": "string",
                "faixa_etaria_demografica": "string",
                "faixa_etaria_legal": "string",
                "profissao": "string",
                "grau_de_instrucao": "string",
                "nacionalidade": "string",
                "ano_sinistro": "Int64",
                "mes_sinistro": "Int64",
                "dia_sinistro": "Int64",
                "ano_mes_sinistro": "string",
                "ano_obito": "Int64",
                "mes_obito": "Int64",
                "dia_obito": "Int64",
                "ano_mes_obito": "string",
                "local_obito": "string",
                "local_via": "string",
                "tempo_sinistro_obito": "Int64",
            }
            date_columns = ["data_sinistro", "data_obito"]
            time_columns = []
        else:
            dtype_dict = {
                "id_sinistro": "Int64",
                "id_veiculo": "Int64",
                "marca_modelo": "string",
                "ano_fab": "Int64",
                "ano_modelo": "Int64",
                "cor_veiculo": "string",
                "tipo_veiculo": "string",
                "ano_sinistro": "Int64",
                "mes_sinistro": "Int64",
                "dia_sinistro": "Int64",
                "ano_mes_sinistro": "string",
            }
            date_columns = ["data_sinistro"]
            time_columns = []
        
        # Read all columns as string initially
        df = pd.read_csv(
            path_file,
            sep=";",
            encoding="latin1",
            dtype="string",
            parse_dates=False,
            keep_default_na=False
        )
        
        # Separate columns by type
        float_columns = [col for col, dtype in dtype_dict.items() if dtype == "Float64"]
        int_columns = [col for col, dtype in dtype_dict.items() if dtype == "Int64"]
        string_columns = [col for col, dtype in dtype_dict.items() if dtype == "string"]
        
        # Convert float columns (handle comma as decimal separator)
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ".", regex=False),
                    errors="coerce"
                ).astype("Float64")
        
        # Convert int columns (handle comma as decimal separator)
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ".", regex=False),
                    errors="coerce"
                ).astype("Int64")
        
        # Keep string columns as string
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype("string")
        
        # Convert date columns
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
        
        # Convert time columns
        for col in time_columns:
            if col in df.columns:
                df[col] = pd.to_timedelta(df[col].astype(str), errors="coerce")
        
        return df
    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
