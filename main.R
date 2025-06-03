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

create_valid_data <- function() {
    list(
        "lista_tipo_via" = c("NAO DISPONIVEL", "VIAS MUNICIPAIS", "RODOVIAS"),
        "lista_tipo_veiculo_vitima" = c(
            "PEDESTRE",
            "AUTOMOVEL",
            "CAMINHAO",
            "MOTOCICLETA",
            "BICICLETA",
            "OUTROS",
            "NAO DISPONIVEL",
            "ONIBUS",
            NA_character_
        ),
        "lista_sexo" = c("FEMININO", "MASCULINO", "NAO DISPONIVEL"),
        "lista_gravidade_lesao" = c("LEVE", "GRAVE", "FATAL", "NAO DISPONIVEL"),
        "lista_tipo_vitima" = c(
            "PEDESTRE",
            "CONDUTOR",
            "PASSAGEIRO",
            "NAO DISPONIVEL",
            NA_character_
        ),
        "lista_faixa_etaria_demografica" = c(
            "00 a 04",
            "05 a 09",
            '10 a 14',
            '15 a 19',
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
        ),
        "lista_faixa_etaria_legal" = c(
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
        ),
        "lista_tipo_veiculo" = c(
            "PEDESTRE",
            "AUTOMOVEL",
            "CAMINHAO",
            "MOTOCICLETA",
            "BICICLETA",
            "OUTROS",
            "NAO DISPONIVEL",
            "ONIBUS"
        )
    )
}

create_schema_pessoas <- function() {
    col_schema(
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
}

create_pessoas_agent <- function(
    df_pessoas,
    valid_data,
    data_release,
    schema,
    lista_municipios,
    path
) {
    options(scipen = 9999999)

    create_agent(
        tbl = df_pessoas,
        tbl_name = "pessoas",
        lang = "pt",
        locale = "pt_BR",
        actions = action_levels(warn_at = 1, stop_at = 0.1)
    ) |>
        col_schema_match(
            schema = schema,
            label = "Tipo de dados"
        ) |>
        col_vals_expr(
            expr = ~ nchar(as.character(id_sinistro)) == 7,
            brief = "`id_sinistro` deve ter 7 dígitos",
            label = "Tamanho de `id_sinistro`"
        ) |>
        col_vals_not_null(
            columns = id_sinistro,
            label = "`id_sinistro` não pode ter vazios"
        ) |>
        col_vals_in_set(
            columns = municipio,
            set = lista_municipios,
            label = "Valida o nome dos municípios"
        ) |>
        col_vals_in_set(
            columns = tipo_via,
            set = valid_data$lista_tipo_via,
            label = "Garante os valores de `tipo_via`"
        ) |>
        col_vals_in_set(
            columns = tipo_veiculo_vitima,
            set = valid_data$lista_tipo_veiculo_vitima,
            label = "Garante os valores de `tipo_veiculo_vitima`"
        ) |>
        col_vals_in_set(
            columns = sexo,
            set = valid_data$lista_sexo,
            label = "Garante os valores de `sexo`"
        ) |>
        col_vals_gte(
            columns = idade,
            value = 0,
            na_pass = TRUE,
            label = "Valor mínimo da idade"
        ) |>
        col_vals_between(
            columns = data_obito,
            na_pass = TRUE,
            right = floor_date(data_release, "month") - days(1),
            left = as.Date("2015-01-01"),
            label = "min/max de `data_obito`"
        ) |>
        col_vals_in_set(
            columns = gravidade_lesao,
            set = valid_data$lista_gravidade_lesao,
            label = "Inputs válidos de `gravidade_lesao`"
        ) |>
        col_vals_in_set(
            columns = `tipo_de vítima`,
            set = valid_data$lista_tipo_vitima,
            label = "Inputs válidos de `tipo_vitima`"
        ) |>
        col_vals_in_set(
            columns = faixa_etaria_demografica,
            set = valid_data$lista_faixa_etaria_demografica,
            label = "Inputs válidos de `faixa_etaria_demografica`"
        ) |>
        col_vals_in_set(
            columns = faixa_etaria_legal,
            set = valid_data$lista_faixa_etaria_legal,
            label = "Inputs válidos de `faixa_etaria_legal`"
        ) |>
        col_vals_between(
            columns = data_sinistro,
            left = as.Date("2014-12-21"),
            right = floor_date(data_release, "month") - days(1),
            label = "min/max de `data_sinistro`"
        ) |>
        col_vals_between(
            columns = ano_sinistro,
            left = 2014,
            right = year(floor_date(data_release, "month") - days(1)),
            label = "min/max de `ano_sinistro`"
        ) |>
        col_vals_between(
            columns = mes_sinistro,
            left = 1,
            right = 12,
            label = "min/max de `mes_sinistro`"
        ) |>
        col_vals_between(
            columns = dia_sinistro,
            left = 1,
            right = vars(max_dia),
            preconditions = function(x)
                x |> mutate(max_dia = days_in_month(data_sinistro)),
            label = "min/max de `dia_sinistro`"
        ) |>
        col_vals_equal(
            columns = ano_mes_sinistro,
            preconditions = function(x)
                x |> mutate(ano_mes = format(data_sinistro, "%Y/%m")),
            value = vars(ano_mes),
            label = "Validação com base em `data_sinistro`"
        ) |>
        col_vals_between(
            columns = ano_obito,
            left = 2015,
            right = year(floor_date(data_release, "month") - days(1)),
            na_pass = TRUE,
            label = "min/max de `ano_obito`"
        ) |>
        col_vals_between(
            columns = mes_obito,
            left = 1,
            right = 12,
            label = "min/max de `mes_obito`",
            na_pass = TRUE
        ) |>
        col_vals_between(
            columns = dia_obito,
            left = 1,
            right = vars(max_dia),
            preconditions = function(x)
                x |> mutate(max_dia = days_in_month(data_obito)),
            label = "min/max de `dia_obito`",
            na_pass = TRUE
        ) |>
        col_vals_equal(
            columns = ano_mes_obito,
            preconditions = function(x)
                x |> mutate(ano_mes = format(data_obito, "%Y/%m")),
            value = vars(ano_mes),
            na_pass = TRUE,
            label = "Validação com base em `data_obito`"
        ) |>
        interrogate() |>
        get_agent_report(
            title = "Dados abertos Infosiga.SP - Validação da tabela 'pessoas'"
        ) |>
        export_report(
            filename = affix_datetime(path, utc_time = FALSE)
        )
    return(path)
}

create_schema_veiculos <- function() {
    col_schema(
        id_sinistro = "numeric",
        ano_fab = "numeric",
        ano_modelo = "numeric",
        cor_veiculo = "character",
        data_sinistro = "Date",
        ano_sinistro = "numeric",
        mes_sinistro = "numeric",
        dia_sinistro = "numeric",
        ano_mes_sinistro = "character",
        tipo_veiculo = "character"
    )
}

create_veiculos_agent <- function(
    df_veiculos,
    valid_data,
    data_release,
    schema,
    path
) {
    options(scipen = 9999999)

    create_agent(
        tbl = df_veiculos,
        tbl_name = "veiculos",
        lang = "pt",
        locale = "pt_BR",
        actions = action_levels(warn_at = 1, stop_at = 0.1)
    ) |>
        col_schema_match(
            schema = schema,
            label = "Tipo de dados"
        ) |>
        col_vals_expr(
            expr = ~ nchar(as.character(id_sinistro)) == 7,
            brief = "`id_sinistro` deve ter 7 dígitos",
            label = "Tamanho de `id_sinistro`"
        ) |>
        col_vals_not_null(
            columns = id_sinistro,
            label = "`id_sinistro` não pode ter vazios"
        ) |>
        col_vals_between(
            columns = ano_modelo,
            preconditions = function(x)
                x |> mutate(ano_limite = ano_sinistro + 1),
            right = vars(ano_limite),
            left = 1956,
            na_pass = TRUE,
            label = "min/max de `ano_modelo` - desconsidera os vazios."
        ) |>
        col_vals_between(
            columns = data_sinistro,
            left = as.Date("2014-12-21"),
            right = floor_date(data_release, "month") - days(1),
            label = "min/max de `data_sinistro`"
        ) |>
        col_vals_between(
            columns = ano_sinistro,
            left = 2014,
            right = year(floor_date(data_release, "month") - days(1)),
            label = "min/max de `ano_sinistro`"
        ) |>
        col_vals_between(
            columns = mes_sinistro,
            left = 1,
            right = 12,
            label = "min/max de `mes_sinistro`"
        ) |>
        col_vals_between(
            columns = dia_sinistro,
            left = 1,
            right = vars(max_dia),
            preconditions = function(x)
                x |> mutate(max_dia = days_in_month(data_sinistro)),
            label = "min/max de `dia_sinistro`"
        ) |>
        col_vals_equal(
            columns = ano_mes_sinistro,
            preconditions = function(x)
                x |> mutate(ano_mes = format(data_sinistro, "%Y/%m")),
            value = vars(ano_mes),
            label = "Validação com base em `data_sinistro`"
        ) |>
        col_vals_in_set(
            columns = tipo_veiculo,
            set = valid_data$lista_tipo_veiculo,
            label = "Inputs válidos de `tipo_veiculo`"
        ) |>
        interrogate() |>
        get_agent_report(
            title = "Dados abertos Infosiga.SP - Validação da tabela 'veiculos'"
        ) |>
        export_report(
            filename = affix_datetime(path, utc_time = FALSE)
        )
}
