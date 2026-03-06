import pandas as pd
import pointblank as pb
from datetime import date, timedelta
from calendar import monthrange
from typing import Dict, List
from schemas import create_valid_data, load_municipios


def days_in_month(dt: pd.Series) -> pd.Series:
    """Calculate days in month for a Series of dates."""
    return pd.Series([monthrange(d.year, d.month)[1] if pd.notna(d) else 0 for d in dt], index=dt.index)


def create_pessoas_agent(
    df_pessoas: pd.DataFrame,
    valid_data: Dict,
    data_release: date,
    schema: pb.Schema,
    lista_municipios: List[str],
) -> str:
    import sys
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    print(f"[PESSOAS] Iniciando validação de {len(df_pessoas)} registros", file=sys.stderr)
    data_max = (datetime.combine(data_release, datetime.min.time()) + relativedelta(months=1) - timedelta(days=1)).date()
    data_min = date(2014, 12, 21)

    print(f"[PESSOAS] Criando agente de validação", file=sys.stderr)
    agent = (
        pb.Validate(
            data=df_pessoas,
            tbl_name="pessoas",
            lang="pt",
            locale="pt",
            thresholds=pb.Thresholds(warning=1, error=0.1)
        )
        .col_schema_match(schema=schema, brief="Tipo de dados")
        .col_vals_expr(
            expr=lambda x: x["id_sinistro"].fillna(0).astype(str).str.len() == 7,
            brief="Espera-se que `id_sinistro` tenha 7 dígitos."
        )
        .col_vals_not_null(columns="id_sinistro", brief="`id_sinistro` não deve ter vazios")
        .col_vals_between(
            columns="id_veiculo",
            left=1,
            right=9999999,
            na_pass=True,
            brief="Min/max válidos de id_veiculo"
        )
        .col_vals_expr(
            expr=lambda x: x["cod_ibge"].fillna(0).astype(str).str.len() == 7,
            brief="Espera-se que `cod_ibge` tenha 7 dígitos."
        )
        .col_vals_in_set(
            columns="municipio",
            set=lista_municipios,
            brief="Valida o nome dos municípios"
        )
        .col_vals_in_set(
            columns="regiao_administrativa",
            set=valid_data["lista_regiao_administrativa"],
            brief="Valida o nome das regiões administrativas"
        )
        .col_vals_not_null(
            columns="regiao_administrativa",
            brief="`regiao_administrativa` não deve ter vazios"
        )
        .col_vals_in_set(
            columns="tipo_via",
            set=valid_data["lista_tipo_via"],
            brief="Garante os valores de `tipo_via`"
        )
        .col_vals_in_set(
            columns="tipo_veiculo_vitima",
            set=valid_data["lista_tipo_veiculo_vitima"],
            brief="Garante os valores de `tipo_veiculo_vitima`"
        )
        .col_vals_in_set(
            columns="sexo",
            set=valid_data["lista_sexo"],
            brief="Garante os valores de `sexo`"
        )
        .col_vals_ge(
            columns="idade",
            value=0,
            na_pass=True,
            brief="Esperam-se valores de `idade`>=0."
        )
        .col_vals_in_set(
            columns="gravidade_lesao",
            set=valid_data["lista_gravidade_lesao"],
            brief="Inputs válidos de `gravidade_lesao`"
        )
        .col_vals_in_set(
            columns="tipo_de_vitima",
            set=valid_data["lista_tipo_vitima"],
            brief="Inputs válidos de `tipo_de_vitima`"
        )
        .col_vals_in_set(
            columns="faixa_etaria_demografica",
            set=valid_data["lista_faixa_etaria_demografica"],
            brief="Inputs válidos de `faixa_etaria_demografica`"
        )
        .col_vals_in_set(
            columns="faixa_etaria_legal",
            set=valid_data["lista_faixa_etaria_legal"],
            brief="Inputs válidos de `faixa_etaria_legal`"
        )
        .col_vals_in_set(
            columns="grau_de_instrucao",
            set=[v for v in valid_data["lista_grau_de_instrucao"] if v is not None],
            brief="Inputs válidos de `grau_de_instrucao`"
        )
        .col_vals_between(
            columns="data_sinistro",
            left=data_min,
            right=data_max,
            brief="min/max de `data_sinistro`"
        )
        .col_vals_between(
            columns="ano_sinistro",
            left=2014,
            right=data_max.year,
            brief="min/max de `ano_sinistro`"
        )
        .col_vals_between(
            columns="mes_sinistro",
            left=1,
            right=12,
            brief="min/max de `mes_sinistro`"
        )
        .col_vals_between(
            columns="data_obito",
            na_pass=True,
            right=data_max,
            left=date(2015, 1, 1),
            brief="min/max de `data_obito`"
        )
        .col_vals_between(
            columns="ano_obito",
            left=2015,
            right=data_max.year,
            na_pass=True,
            brief="min/max de `ano_obito`"
        )
        .col_vals_between(
            columns="mes_obito",
            left=1,
            right=12,
            na_pass=True,
            brief="min/max de `mes_obito`"
        )
        .col_vals_in_set(
            columns="local_obito",
            set=valid_data["lista_local_obito"],
            brief="Valida com base em `local_obito`"
        )
        .col_vals_in_set(
            columns="local_via",
            set=valid_data["lista_tipo_local"],
            brief="Valida com base em `tipo_local`"
        )
        .col_vals_between(
            columns="tempo_sinistro_obito",
            right=30,
            left=0,
            na_pass=True,
            brief="min/max de `tempo_sinistro_obito` - desconsidera os vazios."
        )
    )
    
    # Validações condicionais para casos FATAIS usando expressões
    agent = (
        agent
        .col_vals_expr(
            expr=lambda x: (x["gravidade_lesao"] != "FATAL") | (x["data_obito"].notna()),
            brief="`data_obito` não deve ter vazios quando `gravidade_lesao` é 'FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["gravidade_lesao"] != "FATAL") | (x["ano_obito"].notna()),
            brief="`ano_obito` não deve ter vazios quando `gravidade_lesao` é 'FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["gravidade_lesao"] != "FATAL") | (x["mes_obito"].notna()),
            brief="`mes_obito` não deve ter vazios quando `gravidade_lesao` é 'FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["gravidade_lesao"] != "FATAL") | (x["dia_obito"].notna()),
            brief="`dia_obito` não deve ter vazios quando `gravidade_lesao` é 'FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["gravidade_lesao"] != "FATAL") | (x["ano_mes_obito"].notna()),
            brief="`ano_mes_obito` não deve ter vazios quando `gravidade_lesao` é 'FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["gravidade_lesao"] != "FATAL") | (x["local_obito"].notna()),
            brief="`local_obito` não deve ter vazios quando `gravidade_lesao` é 'FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["gravidade_lesao"] != "FATAL") | (x["tempo_sinistro_obito"].notna()),
            brief="`tempo_sinistro_obito` não deve ter vazios quando `gravidade_lesao` é 'FATAL'"
        )
    )

    print("[PESSOAS] Executando interrogate()...", file=sys.stderr)
    agent = agent.interrogate()
    print("[PESSOAS] Gerando relatório tabular...", file=sys.stderr)
    report = agent.get_tabular_report(title="Dados abertos Infosiga - Validação da tabela 'pessoas'")
    print("[PESSOAS] Convertendo para HTML...", file=sys.stderr)
    html_result = report._repr_html_()
    print(f"[PESSOAS] Validação concluída. HTML size: {len(html_result)} chars", file=sys.stderr)
    return html_result


def create_veiculos_agent(
    df_veiculos: pd.DataFrame,
    valid_data: Dict,
    data_release: date,
    schema: pb.Schema,
) -> str:
    import sys
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    print(f"[VEICULOS] Iniciando validação de {len(df_veiculos)} registros", file=sys.stderr)
    data_max = (datetime.combine(data_release, datetime.min.time()) + relativedelta(months=1) - timedelta(days=1)).date()
    
    agent = (
        pb.Validate(
            data=df_veiculos,
            tbl_name="veiculos",
            lang="pt",
            locale="pt",
            thresholds=pb.Thresholds(warning=1, error=0.1)
        )
        .col_schema_match(schema=schema, brief="Tipo de dados")
        .col_vals_expr(
            expr=lambda x: x["id_sinistro"].fillna(0).astype(str).str.len() == 7,
            brief="Espera-se que `id_sinistro` tenha 7 dígitos."
        )
        .col_vals_not_null(columns="id_sinistro", brief="`id_sinistro` não deve ter vazios")
        .col_vals_between(
            columns="id_veiculo",
            left=1,
            right=9999999,
            na_pass=True,
            brief="Min/max válidos de id_veiculo"
        )
        .col_vals_not_null(columns="id_veiculo", brief="`id_veiculo` não deve ter vazios")
        .col_vals_between(
            columns="ano_fab",
            right=int(df_veiculos["ano_sinistro"].max()),
            left=1956,
            na_pass=True,
            brief="min/max de `ano_fab` - desconsidera os vazios."
        )
        .col_vals_between(
            columns="data_sinistro",
            left=date(2014, 12, 21),
            right=data_max,
            brief="min/max de `data_sinistro`"
        )
        .col_vals_between(
            columns="ano_sinistro",
            left=2014,
            right=data_max.year,
            brief="min/max de `ano_sinistro`"
        )
        .col_vals_between(
            columns="mes_sinistro",
            left=1,
            right=12,
            brief="min/max de `mes_sinistro`"
        )
        .col_vals_in_set(
            columns="tipo_veiculo",
            set=valid_data["lista_tipo_veiculo"],
            brief="Inputs válidos de `tipo_veiculo`"
        )
    )

    print("[VEICULOS] Executando interrogate()...", file=sys.stderr)
    agent = agent.interrogate()
    print("[VEICULOS] Gerando relatório tabular...", file=sys.stderr)
    report = agent.get_tabular_report(title="Dados abertos Infosiga - Validação da tabela 'veiculos'")
    print("[VEICULOS] Convertendo para HTML...", file=sys.stderr)
    html_result = report._repr_html_()
    print(f"[VEICULOS] Validação concluída. HTML size: {len(html_result)} chars", file=sys.stderr)
    return html_result


def create_sinistros_agent(
    df_sinistros: pd.DataFrame,
    valid_data: Dict,
    data_release: date,
    schema: pb.Schema,
    lista_municipios: List[str],
) -> str:
    import sys
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    print(f"[SINISTROS] Iniciando validação de {len(df_sinistros)} registros", file=sys.stderr)
    data_max = (datetime.combine(data_release, datetime.min.time()) + relativedelta(months=1) - timedelta(days=1)).date()
    data_min = date(2014, 12, 21)
    
    agent = (
        pb.Validate(
            data=df_sinistros,
            tbl_name="sinistros",
            lang="pt",
            locale="pt",
            thresholds=pb.Thresholds(warning=1, error=0.1)
        )
        .col_schema_match(schema=schema, brief="Tipo de dados")
        .col_vals_expr(
            expr=lambda x: x["id_sinistro"].fillna(0).astype(str).str.len() == 7,
            brief="Espera-se que `id_sinistro` tenha 7 dígitos."
        )
        .col_vals_not_null(columns="id_sinistro", brief="`id_sinistro` não deve ter vazios")
        .col_vals_expr(
            expr=lambda x: ~x["id_sinistro"].duplicated(keep=False),
            brief="`id_sinistro` deve ser um valor único"
        )
        .col_vals_in_set(
            columns="tipo_registro",
            set=valid_data["lista_tipo_registro"],
            brief="Esperam-se os valores 'SINISTRO FATAL', 'SINISTRO NAO FATAL' e 'NOTIFICACAO'."
        )
        .col_vals_between(
            columns="data_sinistro",
            left=data_min,
            right=data_max,
            brief="min/max de `data_sinistro`"
        )
        .col_vals_between(
            columns="ano_sinistro",
            left=2014,
            right=data_max.year,
            brief="min/max de `ano_sinistro`"
        )
        .col_vals_between(
            columns="mes_sinistro",
            left=1,
            right=12,
            brief="min/max de `mes_sinistro`"
        )
        .col_vals_between(
            columns="dia_sinistro",
            left=1,
            right=data_max.day,
            brief="min/max de `dia_sinistro`"
        )
        .col_vals_expr(
            expr=lambda x: (x["hora_sinistro"] >= pd.Timedelta(0)) & (x["hora_sinistro"] <= pd.Timedelta(hours=23, minutes=59, seconds=59)),
            brief="`hora_sinistro` deve estar entre 00:00:00 e 23:59:59"
        )
        .col_vals_expr(
            expr=lambda x: x["ano_mes_sinistro"] == x["ano_sinistro"].astype(str) + "/" + x["mes_sinistro"].astype(str).str.zfill(2),
            brief="`ano_mes_sinistro` deve ser a concatenação de `ano_sinistro` com '/' e `mes_sinistro`"
        )
        .col_vals_in_set(
            columns="dia_da_semana",
            set=valid_data["lista_dia_semana"],
            brief="Inputs válidos de `dia_semana`"
        )
        .col_vals_not_null(columns="dia_da_semana", brief="`dia_da_semana` não deve ter vazios")
        .col_vals_in_set(
            columns="turno",
            set=valid_data["lista_turno"],
            brief="Inputs válidos de `turno`"
        )
        .col_vals_not_null(columns="turno", brief="`turno` não deve ter vazios")
        .col_vals_in_set(
            columns="tipo_via",
            set=valid_data["lista_tipo_via"],
            brief="Inputs válidos de `tipo_via`"
        )
        .col_vals_not_null(columns="tipo_via", brief="`tipo_via` não deve ter vazios")
        .col_vals_in_set(
            columns="tipo_local",
            set=valid_data["lista_tipo_local"],
            brief="Inputs válidos de `tipo_local`"
        )
        .col_vals_between(
            columns="latitude",
            left=-25.31,
            right=-19.77,
            na_pass=True,
            brief="Min/max válidos de latitude."
        )
        .col_vals_between(
            columns="longitude",
            left=-53.11,
            right=-44.16,
            na_pass=True,
            brief="Min/max válidos de longitude."
        )
        .col_vals_in_set(
            columns="municipio",
            set=lista_municipios,
            brief="Valores válidos de nome de município."
        )
        .col_vals_not_null(columns="municipio", brief="`municipio` não deve ter vazios")
        .col_vals_in_set(
            columns="regiao_administrativa",
            set=valid_data["lista_regiao_administrativa"],
            brief="Inputs válidos de região administrativa."
        )
        .col_vals_not_null(columns="regiao_administrativa", brief="`regiao_administrativa` não deve ter vazios")
        .col_vals_in_set(
            columns="administracao",
            set=valid_data["lista_administracao"],
            brief="Inputs válidos de `adminstracao`"
        )
        .col_vals_in_set(
            columns="circunscricao",
            set=valid_data["lista_circunscricao"],
            brief="Inputs válidos de `circunscricao`"
        )
        .col_vals_in_set(
            columns="tp_sinistro_primario",
            set=valid_data["lista_tp_sinistro_primario"],
            brief="Inputs válidos de tp_sinistro_primario."
        )
        .col_vals_not_null(columns="tp_sinistro_primario", brief="`tp_sinistro_primario` não deve ter vazios")
        .col_vals_gt(columns="qtd_pedestre", value=0, brief="`qtd_pedestre` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_bicicleta", value=0, brief="`qtd_bicicleta` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_motocicleta", value=0, brief="`qtd_motocicleta` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_automovel", value=0, brief="`qtd_automovel` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_onibus", value=0, brief="`qtd_onibus` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_caminhao", value=0, brief="`qtd_caminhao` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_veic_outros", value=0, brief="`qtd_veic_outros` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_veic_nao_disponivel", value=0, brief="`qtd_veic_nao_disponivel` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_gravidade_fatal", value=0, brief="`qtd_gravidade_fatal` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_gravidade_grave", value=0, brief="`qtd_gravidade_grave` > 0", na_pass=True)
        .col_vals_gt(columns="qtd_gravidade_leve", value=0, brief="`qtd_gravidade_leve` > 0", na_pass=True)
        .col_vals_null(columns="qtd_gravidade_ileso", brief="`qtd_gravidade_ileso` é sempre vazio.")
        .col_vals_gt(columns="qtd_gravidade_nao_disponivel", value=0, brief="`qtd_gravidade_nao_disponivel` > 0", na_pass=True)
    )
    
    # Validações condicionais usando expressões
    agent = (
        agent
        # Validações de latitude/longitude por tipo_registro
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] != "SINISTRO FATAL") | (x["latitude"].notna()),
            brief="`latitude` não deve ter vazios para 'SINISTRO FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] != "SINISTRO FATAL") | (x["longitude"].notna()),
            brief="`longitude` não deve ter vazios para 'SINISTRO FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] != "SINISTRO NAO FATAL") | (x["latitude"].notna()),
            brief="`latitude` não deve ter vazios para 'SINISTRO NAO FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] != "SINISTRO NAO FATAL") | (x["longitude"].notna()),
            brief="`longitude` não deve ter vazios para 'SINISTRO NAO FATAL'"
        )
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] != "NOTIFICACAO") | (x["latitude"].notna()),
            brief="`latitude` não deve ter vazios para 'NOTIFICACAO'"
        )
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] != "NOTIFICACAO") | (x["longitude"].notna()),
            brief="`longitude` não deve ter vazios para 'NOTIFICACAO'"
        )
        # Validações condicionais para qtd_pedestre quando atropelamento
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] == "NOTIFICACAO") | (x["tp_sinistro_primario"] != "ATROPELAMENTO") | (x["qtd_pedestre"].notna()),
            brief="`qtd_pedestre` não deve ter vazios quando `tp_sinistro_primario` é 'ATROPELAMENTO' (segmentado por tipo_registro)"
        )
        # Validações condicionais para qtd_gravidade_fatal
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] != "SINISTRO FATAL") | (x["qtd_gravidade_fatal"].notna()),
            brief="`qtd_gravidade_fatal` não deve ter vazios quando `tipo_registro` é 'SINISTRO FATAL'"
        )
        # Validações condicionais para tp_sinistro_atropelamento
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] == "NOTIFICACAO") | (x["tp_sinistro_primario"] != "ATROPELAMENTO") | (x["tp_sinistro_atropelamento"].notna()),
            brief="`tp_sinistro_atropelamento` não deve ter vazios quando `tp_sinistro_primario` é 'ATROPELAMENTO' (segmentado por tipo_registro)"
        )
        # Validações condicionais para tp_sinistro_choque
        .col_vals_expr(
            expr=lambda x: (x["tipo_registro"] == "NOTIFICACAO") | (x["tp_sinistro_primario"] != "CHOQUE") | (x["tp_sinistro_choque"].notna()),
            brief="`tp_sinistro_choque` não deve ter vazios quando `tp_sinistro_primario` é 'CHOQUE' (segmentado por tipo_registro)"
        )
        # Ao menos uma colisão marcada quando tp_sinistro_primario é COLISAO
        .col_vals_expr(
            expr=lambda x: (x["tp_sinistro_primario"] != "COLISAO") | (
                (x["tp_sinistro_colisao_frontal"] == "S") |
                (x["tp_sinistro_colisao_traseira"] == "S") |
                (x["tp_sinistro_colisao_lateral"] == "S") |
                (x["tp_sinistro_colisao_transversal"] == "S") |
                (x["tp_sinistro_colisao_outros"] == "S")
            ),
            brief="Ao menos uma variável de colisão deve ter valor 'S' quando `tp_sinistro_primario` é 'COLISAO'"
        )
        # Soma de veículos > 1 quando COLISAO
        .col_vals_expr(
            expr=lambda x: (x["tp_sinistro_primario"] != "COLISAO") | (
                (x["qtd_bicicleta"].fillna(0) + x["qtd_motocicleta"].fillna(0) +
                 x["qtd_automovel"].fillna(0) + x["qtd_onibus"].fillna(0) +
                 x["qtd_caminhao"].fillna(0) + x["qtd_veic_outros"].fillna(0) +
                 x["qtd_veic_nao_disponivel"].fillna(0)) > 1
            ),
            brief="Soma de veículos deve ser > 1 quando `tp_sinistro_primario` é 'COLISAO'"
        )
        # Ao menos um tipo de OUTROS marcado quando tp_sinistro_primario é OUTROS
        .col_vals_expr(
            expr=lambda x: (x["tp_sinistro_primario"] != "OUTROS") | (
                (x["tp_sinistro_capotamento"] == "S") |
                (x["tp_sinistro_engavetamento"] == "S") |
                (x["tp_sinistro_tombamento"] == "S") |
                (x["tp_sinistro_outros"] == "S")
            ),
            brief="Ao menos uma variável deve ter valor 'S' entre capotamento, engavetamento, tombamento e outros quando `tp_sinistro_primario` é 'OUTROS'"
        )
        # tp_sinistro_nao_disponivel não deve ter vazios quando tp_sinistro_primario é NAO DISPONIVEL
        .col_vals_expr(
            expr=lambda x: (x["tp_sinistro_primario"] != "NAO DISPONIVEL") | (x["tp_sinistro_nao_disponivel"].notna()),
            brief="`tp_sinistro_nao_disponivel` não deve ter vazios quando `tp_sinistro_primario` é 'NAO DISPONIVEL'"
        )
    )

    print("[SINISTROS] Executando interrogate()...", file=sys.stderr)
    agent = agent.interrogate()
    print("[SINISTROS] Gerando relatório tabular...", file=sys.stderr)
    report = agent.get_tabular_report(title="Dados abertos Infosiga - Validação da tabela 'sinistros'")
    print("[SINISTROS] Convertendo para HTML...", file=sys.stderr)
    html_result = report._repr_html_()
    print(f"[SINISTROS] Validação concluída. HTML size: {len(html_result)} chars", file=sys.stderr)
    return html_result
