import zipfile
import tempfile
import os
import polars as pl
from typing import Literal


def read_infosiga(path: str, file: Literal["pessoas", "veiculos", "sinistros"]) -> pl.DataFrame:
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        path_files = sorted(
            os.path.join(temp_dir, f)
            for f in os.listdir(temp_dir)
            if file in f.lower()
        )

        if not path_files:
            raise FileNotFoundError(f"Arquivo '{file}' não encontrado no ZIP")

        if file == "sinistros":
            int_columns = [
                "id_sinistro", "ano_sinistro", "mes_sinistro", "dia_sinistro",
                "cod_ibge", "qtd_pedestre", "qtd_bicicleta", "qtd_motocicleta",
                "qtd_automovel", "qtd_onibus", "qtd_caminhao", "qtd_veic_outros",
                "qtd_veic_nao_disponivel", "qtd_gravidade_fatal", "qtd_gravidade_grave",
                "qtd_gravidade_leve", "qtd_gravidade_ileso", "qtd_gravidade_nao_disponivel",
            ]
            float_columns = ["numero_logradouro", "latitude", "longitude"]
            date_columns = ["data_sinistro"]
            time_columns = ["hora_sinistro"]

        elif file == "pessoas":
            int_columns = [
                "id_sinistro", "id_veiculo", "cod_ibge", "idade",
                "ano_sinistro", "mes_sinistro", "dia_sinistro",
                "ano_obito", "mes_obito", "dia_obito", "tempo_sinistro_obito",
            ]
            float_columns = []
            date_columns = ["data_sinistro", "data_obito"]
            time_columns = []

        else:  # veiculos
            int_columns = [
                "id_sinistro", "id_veiculo", "ano_fab", "ano_modelo",
                "ano_sinistro", "mes_sinistro", "dia_sinistro",
            ]
            float_columns = []
            date_columns = ["data_sinistro"]
            time_columns = []

        parts = [
            pl.read_csv(p, separator=";", encoding="latin1", infer_schema=False, null_values=[""])
            for p in path_files
        ]
        df = pl.concat(parts) if len(parts) > 1 else parts[0]

        # Converte inteiros (vírgula como separador decimal)
        int_exprs = [
            pl.col(c).str.replace(",", ".", literal=True).cast(pl.Int64, strict=False).alias(c)
            for c in int_columns if c in df.columns
        ]
        float_exprs = [
            pl.col(c).str.replace(",", ".", literal=True).cast(pl.Float64, strict=False).alias(c)
            for c in float_columns if c in df.columns
        ]
        if int_exprs or float_exprs:
            df = df.with_columns(int_exprs + float_exprs)

        # Converte datas
        date_exprs = [
            pl.col(c).str.strptime(pl.Date, format="%d/%m/%Y", strict=False).alias(c)
            for c in date_columns if c in df.columns
        ]
        if date_exprs:
            df = df.with_columns(date_exprs)

        # Converte hora (HH:MM:SS → pl.Time)
        if time_columns:
            df = df.with_columns([
                pl.col(c).str.strptime(pl.Time, format="%T", strict=False).alias(c)
                for c in time_columns if c in df.columns
            ])

        return df

    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
