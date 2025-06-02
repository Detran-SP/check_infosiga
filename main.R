read_infosiga <- function(path, file = c("pessoas", "veiculos", "sinistros")) {
    tempdir = tempdir()
    path_zip = path
    unzip(path_zip, exdir = tempdir)
    path_file = list.files(tempdir, pattern = file, full.names = TRUE)

    if (file == "sinistros") {
        cols = cols(
            "n", # id_sinistro
            "c", # tipo_registro
            col_date(format = "%d/%m/%Y"), # data_sinistro
            "n", # ano_sinistro
            "n", # mes_sinistro
            "n", # dia_sinistro
            "c", # ano_mes_sinistro
            "t", # hora_sinistro
            "c", # logradouro
            "n", # numero_logradouro
            "c", # tipo_via
            "d", # latitude
            "d", # longitude
            "c", # municipio
            "c", # regiao_administrativa
            "n", # tp_veiculo_bicicleta
            "n", # tp_veiculo_caminhao
            "n", # tp_veiculo_motocicleta
            "n", # tp_veiculo_nao_disponivel
            "n", # tp_veiculo_onibus
            "n", # tp_veiculo_outros
            "n", # tp_veiculo_automovel
            "n", # gravidade_nao_disponivel
            "n", # gravidade_leve
            "n", # gravidade_fatal
            "n", # gravidade_ileso
            "n", # gravidade_grave,
            "c", # administracao
            "c", # conversacao
            "c", # tipo_acidente_primario
            "c", # tp_sinistro_colisao_frontal
            "c", # tp_sinistro_colisao_traseira
            "c", # tp_sinistro_colisao_lateral
            "c", # tp_sinistro_colisao_transversal
            "c", # tp_sinistro_colisao_outros
            "c", # tp_sinistro_choque
            "c", # tp_sinistro_capotamento
            "c", # tp_sinistro_engavetamento
            "c", # tp_sinistro_tombamento
            "c", # tp_sinistro_outros
            "c" # tp_sinistro_nao_disponivel
        )
    }

    if (file == "pessoas") {
        cols = cols(
            "n", # id_sinistro
            "c", # municipio
            "c", # tipo_via
            "c", # tipo_veiculo_via
            "c", # sexo
            "n", # idade
            col_date(format = "%d/%m/%Y"), # data_obito
            "c", # gravidade_lesao
            "c", # `tipo_de vítima`
            "c", # faixa_etaria_demografica
            "c", # faixa_etaria_legal
            "c", # profissao
            col_date(format = "%d/%m/%Y"), # data_sinistro
            "n", # ano_sinistro
            "n", # mes_sinistro
            "n", # dia_sinistro
            "c", # ano_mes_sinistro
            "n", # ano_obito
            "n", # mes_obito
            "n", # dia_obito
            "c" # ano_mes_obito
        )
    }

    if (file == "veiculos") {
        cols = cols(
            "n", # id_sinistro
            "n", # ano_fab
            "n", # ano_modelo
            "c", # cor_veiculo
            col_date(format = "%d/%m/%Y"), # data_sinistro
            "n", # ano_sinistro
            "n", # ano_sinistro
            "n", # mes_sinistro
            "n", # dia_sinistro
            "c", # ano_mes_sinistro
            "c" # tipo_veiculo
        )
    }

    df = readr::read_csv2(
        path_file,
        locale = readr::locale(encoding = "latin1"),
        col_types = cols
    )

    on.exit(unlink(tempdir, recursive = TRUE))
    return(df)
}

tar_load(df_infosiga)

schema_pessoas <- col_schema(
    id_sinistro = "numeric",
    municipio = "character",
    tipo_via = "character",
    tipo_veiculo_vitima = "character",
    sexo = "character",
    idade = "numeric",
    data_obito = "Date",
    gravidade_lesao = "character",
    `tipo_de vítima` = "character",
    faixa_etaria_demografica = "character",
    faixa_etaria_legal = "character",
    profissao = "character",
    data_sinistro = "Date",
    ano_sinistro = "numeric",
    mes_sinistro = "numeric",
    dia_sinistro = "numeric",
    ano_mes_sinistro = "character",
    ano_obito = "numeric",
    mes_obito = "numeric",
    dia_obito = "numeric",
    ano_mes_obito = "character"
)

options(scipen = 9999999)

create_agent(
    tbl = df_infosiga[[1]],
    tbl_name = "pessoas",
    lang = "pt",
    locale = "pt_BR"
) |>
    col_schema_match(
        schema = schema_pessoas,
        label = "Tipo de dados",
        actions = stop_on_fail(1)
    ) |>
    col_vals_expr(
        expr = ~ nchar(as.character(id_sinistro)) == 7,
        brief = "`id_sinistro` deve ter 7 dígitos",
        label = "Tamanho de `id_sinistro`",
        actions = warn_on_fail(warn_at = 1)
    ) |>
    interrogate() |>
    get_agent_report(
        title = "Dados abertos Infosiga.SP - Validação da tabela 'pessoas'"
    )
