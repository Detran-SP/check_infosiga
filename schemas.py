from typing import Dict, List, Any
import json
from pathlib import Path
import pointblank as pb


def create_valid_data() -> Dict[str, List[str]]:
    return {
        "lista_tipo_registro": [
            "SINISTRO FATAL",
            "SINISTRO NAO FATAL",
            "NOTIFICACAO"
        ],
        "lista_dia_semana": [
            "Domingo",
            "Segunda-feira",
            "Terça-feira",
            "Quarta-feira",
            "Quinta-feira",
            "Sexta-feira",
            "Sábado"
        ],
        "lista_turno": [
            "MANHA",
            "TARDE",
            "NOITE",
            "MADRUGADA",
            "NAO DISPONIVEL"
        ],
        "lista_tipo_via": [
            "VIAS URBANAS",
            "ESTRADAS E RODOVIAS",
            "NAO DISPONIVEL"
        ],
        "lista_tipo_local": [
            "PUBLICO",
            "PRIVADO",
            "NAO DISPONIVEL"
        ],
        "lista_regiao_administrativa": [
            "METROPOLITANA DE SÃO PAULO",
            "CAMPINAS",
            "SOROCABA",
            "SÃO JOSÉ DOS CAMPOS",
            "SÃO JOSÉ DO RIO PRETO",
            "BAIXADA SANTISTA",
            "RIBEIRÃO PRETO",
            "CENTRAL",
            "BAURU",
            "MARÍLIA",
            "PRESIDENTE PRUDENTE",
            "ARAÇATUBA",
            "FRANCA",
            "ITAPEVA",
            "REGISTRO",
            "BARRETOS"
        ],
        "lista_administracao": [
            "PREFEITURA",
            "NAO DISPONIVEL",
            "CONCESSIONÁRIA",
            "DER",
            "CONCESSIONÁRIA-ARTESP",
            "DNIT",
            "CONCESSIONÁRIA-ANTT",
            "ARTESP"
        ],
        "lista_circunscricao": [
            "MUNICIPAL",
            "ESTADUAL",
            "FEDERAL",
            "NAO DISPONIVEL"
        ],
        "lista_tp_sinistro_primario": [
            "ATROPELAMENTO",
            "COLISAO",
            "CHOQUE",
            "OUTROS",
            "NAO DISPONIVEL"
        ],
        "lista_tipo_veiculo_vitima": [
            "AUTOMOVEL",
            "BICICLETA",
            "CAMINHAO",
            "MOTOCICLETA",
            "ONIBUS",
            "OUTROS",
            "NAO DISPONIVEL"
        ],
        "lista_sexo": ["FEMININO", "MASCULINO", "NAO DISPONIVEL"],
        "lista_gravidade_lesao": ["FATAL", "GRAVE", "LEVE", "NAO DISPONIVEL"],
        "lista_tipo_vitima": [
            "CONDUTOR",
            "PASSAGEIRO",
            "PEDESTRE",
            "NAO DISPONIVEL"
        ],
        "lista_faixa_etaria_demografica": [
            "00 a 04",
            "05 a 09",
            "10 a 14",
            "15 a 19",
            "20 a 24",
            "25 a 29",
            "30 a 34",
            "35 a 39",
            "40 a 44",
            "45 a 49",
            "50 a 54",
            "55 a 59",
            "60 a 64",
            "65 a 69",
            "70 a 74",
            "75 a 79",
            "80 a 84",
            "85 a 89",
            "90 e +",
            "NAO DISPONIVEL"
        ],
        "lista_faixa_etaria_legal": [
            "0-17",
            "18-24",
            "25-29",
            "30-34",
            "35-39",
            "40-44",
            "45-49",
            "50-54",
            "55-59",
            "60-64",
            "65-69",
            "70-74",
            "75-79",
            "80 ou mais",
            "NAO DISPONIVEL"
        ],
        "lista_grau_de_instrucao": [
            "BÁSICO",
            "MÉDIO",
            "SUPERIOR",
            None
        ],
        "lista_local_obito": [
            "VIA",
            "ESTABELECIMENTO DE SAUDE",
            "OUTROS",
            "NAO DISPONIVEL"
        ],
        "lista_tipo_veiculo": [
            "AUTOMOVEL",
            "BICICLETA",
            "CAMINHAO",
            "MOTOCICLETA",
            "ONIBUS",
            "OUTROS",
            "NAO DISPONIVEL"
        ]
    }


def create_schema_pessoas() -> pb.Schema:
    return pb.Schema(columns={
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
        "data_sinistro": "datetime64[ns]",
        "ano_sinistro": "Int64",
        "mes_sinistro": "Int64",
        "dia_sinistro": "Int64",
        "ano_mes_sinistro": "string",
        "data_obito": "datetime64[ns]",
        "ano_obito": "Int64",
        "mes_obito": "Int64",
        "dia_obito": "Int64",
        "ano_mes_obito": "string",
        "local_obito": "string",
        "local_via": "string",
        "tempo_sinistro_obito": "Int64"
    })


def create_schema_veiculos() -> pb.Schema:
    return pb.Schema(columns={
        "id_sinistro": "Int64",
        "id_veiculo": "Int64",
        "marca_modelo": "string",
        "ano_fab": "Int64",
        "ano_modelo": "Int64",
        "cor_veiculo": "string",
        "tipo_veiculo": "string",
        "data_sinistro": "datetime64[ns]",
        "ano_sinistro": "Int64",
        "mes_sinistro": "Int64",
        "dia_sinistro": "Int64",
        "ano_mes_sinistro": "string"
    })


def create_schema_sinistros() -> pb.Schema:
    return pb.Schema(columns={
        "id_sinistro": "Int64",
        "tipo_registro": "string",
        "data_sinistro": "datetime64[ns]",
        "ano_sinistro": "Int64",
        "mes_sinistro": "Int64",
        "dia_sinistro": "Int64",
        "hora_sinistro": "timedelta64[ns]",
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
        "tp_sinistro_nao_disponivel": "string"
    })


def load_municipios() -> List[str]:
    municipios_path = Path(__file__).parent / "data" / "municipios.json"
    with open(municipios_path, "r", encoding="utf-8") as f:
        return json.load(f)
