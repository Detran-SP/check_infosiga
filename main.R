#' Read Infosiga data from ZIP file
#'
#' Extracts and reads the specified file ("pessoas", "veiculos" or
#' "sinistros") from a ZIP archive, applying the correct column types.
#'
#' @param path A string. Path to the ZIP file containing the data.
#' @param file A string. One of "pessoas", "veiculos", or "sinistros".
#'
#' @return A data frame with parsed columns.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' df <- read_infosiga("data/infosiga.zip", "sinistros")
#' }
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
            "t", # hora_sinistro
            "c", # ano_mes_sinistro
            "c", # dia_da_semana
            "c", # turno

            "c", # logradouro
            "n", # numero_logradouro
            "c", # tipo_via
            "c", # tipo_local

            "d", # latitude
            "d", # longitude
            "n", # cod_ibge
            "c", # municipio
            "c", # regiao_administrativa

            "c", # administracao
            "c", # conservacao
            "c", # circunscricao
            "c", # tp_sinistro_primario

            "n", # qtd_pedestre
            "n", # qtd_bicicleta
            "n", # qtd_motocicleta
            "n", # qtd_automovel
            "n", # qtd_onibus
            "n", # qtd_caminhao
            "n", # qtd_veic_outros
            "n", # qtd_veic_nao_disponivel

            "n", # qtd_gravidade_fatal
            "n", # qtd_gravidade_grave,
            "n", # qtd_gravidade_leve
            "n", # qtd_gravidade_ileso
            "n", # qtd_gravidade_nao_disponivel

            "c", # tp_sinistro_atropelamento
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
            "n", # id_veiculo

            "n", # cod_ibge
            "c", # municipio
            "c", # regiao_administrativa
            "c", # tipo_via

            "c", # tipo_veiculo_vitima
            "c", # sexo
            "n", # idade
            "c", # gravidade_lesao

            "c", # tipo_de_vitima
            "c", # faixa_etaria_demografica
            "c", # faixa_etaria_legal
            "c", # profissao
            "c", # grau de instrução
            "c", # nacionalidade

            col_date(format = "%d/%m/%Y"), # data_sinistro
            "n", # ano_sinistro
            "n", # mes_sinistro
            "n", # dia_sinistro
            "c", # ano_mes_sinistro

            col_date(format = "%d/%m/%Y"), # data_obito
            "n", # ano_obito
            "n", # mes_obito
            "n", # dia_obito
            "c", # ano_mes_obito
            "c", # local_obito
            "n" # tempo_sinistro_obito
        )
    }

    if (file == "veiculos") {
        cols = cols(
            "n", # id_sinistro
            "n", # id_veiculo

            "c", # marca_modelo
            "n", # ano_fab
            "n", # ano_modelo
            "c", # cor_veiculo
            "c", # tipo_veiculo

            col_date(format = "%d/%m/%Y"), # data_sinistro
            "n", # ano_sinistro
            "n", # mes_sinistro
            "n", # dia_sinistro
            "c" # ano_mes_sinistro
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

#' Create reference lists for validation
#'
#' Returns predefined sets of valid values used in data validation rules for
#' Infosiga datasets.
#'
#' @return A named list of character vectors with valid values for each field.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' valid <- create_valid_data()
#' valid$lista_tipo_via
#' }
create_valid_data <- function() {
    list(
        "lista_regiao_administrativa" = c(
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
        ),
        "lista_dia_semana" = c(
            "Domingo",
            "Segunda-feira",
            "Terça-feira",
            "Quarta-feira",
            "Quinta-feira",
            "Sexta-feira",
            "Sábado"
        ),
        "lista_turno" = c(
            "MANHA",
            "TARDE",
            "NOITE",
            "MADRUGADA"
        ),

        "lista_tipo_via" = c(
            "NAO DISPONIVEL",
            "VIAS URBANAS",
            "ESTRADAS E RODOVIAS"
        ),

        "lista_tipo_veiculo_vitima" = c(
            #"PEDESTRE",
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
        "lista_grau_de_instrucao" = c(
            "BÁSICO",
            "MÉDIO",
            "SUPERIOR",
            NA_character_
        ),

        "lista_local_obito" = c(
            "VIA",
            "ESTABELECIMENTO DE SAUDE",
            "OUTROS",
            "NAO DISPONIVEL",
            NA_character_
        ),

        "lista_tipo_local" = c("PUBLICO", "PRIVADO", "NAO DISPONIVEL"),

        "lista_tipo_veiculo" = c(
            #"PEDESTRE",
            "AUTOMOVEL",
            "CAMINHAO",
            "MOTOCICLETA",
            "BICICLETA",
            "OUTROS",
            "NAO DISPONIVEL",
            "ONIBUS"
        ),
        "lista_tipo_registro" = c(
            "SINISTRO FATAL",
            "NOTIFICACAO",
            "SINISTRO NAO FATAL"
        ),
        "lista_administracao" = c(
            "PREFEITURA",
            "NAO DISPONIVEL",
            'CONCESSIONÁRIA',
            "DER",
            "CONCESSIONÁRIA-ARTESP",
            "DNIT",
            "CONCESSIONÁRIA-ANTT",
            "ARTESP",
            NA_character_
        ),
        "lista_circunscricao" = c(
            "MUNICIPAL",
            "NAO DISPONIVEL",
            "ESTADUAL",
            "FEDERAL",
            NA_character_
        ),
        "lista_tp_sinistro_primario" = c(
            "ATROPELAMENTO",
            "NAO DISPONIVEL",
            "OUTROS",
            "COLISAO",
            "CHOQUE"
        ),
        "lista_conservacao" = c(
            "Regional DER (Rodovia Estadual)",
            "DNIT (Rodovia Federal)",
            "Concessionária da Rodovia (Estadual e Federal)",
            "Prefeitura (Municipal)",
            "NAO DISPONIVEL",
            NA_character_
        )
    )
}

#' Create schema for 'pessoas' table
#'
#' Defines the expected structure and column types for the 'pessoas' dataset.
#'
#' @return A pointblank `col_schema` object specifying column names and types.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' schema <- create_schema_pessoas()
#' }
create_schema_pessoas <- function() {
    col_schema(
        id_sinistro = "numeric",
        id_veiculo = "numeric",
        cod_ibge = "numeric",
        municipio = "character",
        regiao_administrativa = "character",
        tipo_via = "character",
        tipo_veiculo_vitima = "character",
        sexo = "character",
        idade = "numeric",
        gravidade_lesao = "character",
        tipo_de_vitima = "character",
        faixa_etaria_demografica = "character",
        faixa_etaria_legal = "character",
        profissao = "character",
        grau_de_instrucao = "character",
        nacionalidade = "character",
        data_sinistro = "Date",
        ano_sinistro = "numeric",
        mes_sinistro = "numeric",
        dia_sinistro = "numeric",
        ano_mes_sinistro = "character",
        data_obito = "Date",
        ano_obito = "numeric",
        mes_obito = "numeric",
        dia_obito = "numeric",
        ano_mes_obito = "character",
        local_obito = "character",
        tempo_sinistro_obito = "numeric"
    )
}

#' Validate 'pessoas' dataset using pointblank
#'
#' Runs a series of data validation checks on the 'pessoas' dataset from
#' Infosiga using rules defined via the pointblank package.
#'
#' @param df_pessoas A data frame with the raw 'pessoas' dataset.
#' @param valid_data A named list of valid reference values (e.g. from
#' `create_valid_data()`).
#' @param data_release A Date indicating the current data reference cutoff.
#' @param schema A col_schema object defining expected column types.
#' @param lista_municipios A character vector with valid municipality names.
#' @param path Path to save the validation report (HTML).
#'
#' @return The full path to the validation report file.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' create_pessoas_agent(
#'     df_pessoas, valid_data, Sys.Date(), schema,
#'     lista_municipios, "relatorios/pessoas.html"
#' )
#' }
create_pessoas_agent <- function(
    df_pessoas,
    valid_data,
    data_release,
    schema,
    lista_municipios,
    path
) {
    options(scipen = 9999)

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
        col_vals_between(
            columns = id_veiculo,
            left = 1,
            right = 9999999,
            na_pass = TRUE,
            label = "Min/max válidos de id_veiculo"
        ) |>
        col_vals_expr(
            expr = ~ nchar(as.character(cod_ibge)) == 7,
            brief = "`cod_ibge` deve ter 7 dígitos",
            label = "Tamanho do `cod_ibge`"
        ) |>
        col_vals_in_set(
            columns = municipio,
            set = lista_municipios,
            label = "Valida o nome dos municípios"
        ) |>
        col_vals_in_set(
            columns = municipio,
            set = lista_municipios,
            label = "Valida o nome dos municípios"
        ) |>
        col_vals_in_set(
            columns = regiao_administrativa,
            set = valid_data$lista_regiao_administrativa,
            label = "Valida o nome das regiões administrativas"
        ) |>
        col_vals_not_null(
            columns = regiao_administrativa,
            label = "`regiao_administrativa` não pode ter vazios"
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
        col_vals_in_set(
            columns = gravidade_lesao,
            set = valid_data$lista_gravidade_lesao,
            label = "Inputs válidos de `gravidade_lesao`"
        ) |>
        col_vals_in_set(
            columns = tipo_de_vitima,
            set = valid_data$lista_tipo_vitima,
            label = "Inputs válidos de `tipo_de_vitima`"
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
        col_vals_in_set(
            columns = grau_de_instrucao,
            set = valid_data$lista_grau_de_instrucao,
            label = "Inputs válidos de `grau_de_instrucao`"
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
            preconditions = function(x) {
                x |> mutate(max_dia = days_in_month(data_sinistro))
            },
            label = "min/max de `dia_sinistro`"
        ) |>
        col_vals_equal(
            columns = ano_mes_sinistro,
            preconditions = function(x) {
                x |> mutate(ano_mes = format(data_sinistro, "%Y/%m"))
            },
            value = vars(ano_mes),
            label = "Validação com base em `data_sinistro`"
        ) |>
        col_vals_between(
            columns = data_obito,
            na_pass = TRUE,
            right = floor_date(data_release, "month") - days(1),
            left = as.Date("2015-01-01"),
            label = "min/max de `data_obito`"
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
            preconditions = function(x) {
                x |> mutate(max_dia = days_in_month(data_obito))
            },
            label = "min/max de `dia_obito`",
            na_pass = TRUE
        ) |>
        col_vals_equal(
            columns = ano_mes_obito,
            preconditions = function(x) {
                x |> mutate(ano_mes = format(data_obito, "%Y/%m"))
            },
            value = vars(ano_mes),
            na_pass = TRUE,
            label = "Validação com base em `data_obito`"
        ) |>
        col_vals_in_set(
            columns = local_obito,
            set = valid_data$lista_local_obito,
            label = "Valida com base em `local_obito`"
        ) |>
        col_vals_not_null(
            columns = vars(local_obito),
            preconditions = ~ . %>% filter(gravidade_lesao == "FATAL"),
            label = "`local_obito` não pode ter vazios quando `gravidade_lesao` é 'FATAL'"
        ) |>
        #col_vals_gte(
        #    columns = tempo_sinistro_obito,
        #    value = 0,
        #    na_pass = TRUE,
        #    label = "Valor mínimo de 'tempo_sinistro_obito'"
        #) |>
        col_vals_between(
            columns = tempo_sinistro_obito,
            right = 30,
            left = 0,
            na_pass = TRUE,
            label = "min/max de `tempo_sinistro_obito` - desconsidera os vazios."
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

#' Create schema for 'veiculos' table
#'
#' Defines the expected structure and column types for the 'veiculos' dataset.
#'
#' @return A pointblank `col_schema` object specifying column names and types.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' schema <- create_schema_veiculos()
#' }
create_schema_veiculos <- function() {
    col_schema(
        id_sinistro = "numeric",
        id_veiculo = "numeric",
        marca_modelo = "character",
        ano_fab = "numeric",
        ano_modelo = "numeric",
        cor_veiculo = "character",
        tipo_veiculo = "character",
        data_sinistro = "Date",
        ano_sinistro = "numeric",
        mes_sinistro = "numeric",
        dia_sinistro = "numeric",
        ano_mes_sinistro = "character"
    )
}

#' Validate 'veiculos' dataset using pointblank
#'
#' Runs validation rules on the 'veiculos' dataset from Infosiga using the
#' pointblank package.
#'
#' @param df_veiculos A data frame with the raw 'veiculos' dataset.
#' @param valid_data A named list of valid reference values (e.g. from
#' `create_valid_data()`).
#' @param data_release A Date indicating the current data reference cutoff.
#' @param schema A col_schema object defining expected column types.
#' @param path Path to save the validation report (HTML).
#'
#' @return The full path to the validation report file.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' create_veiculos_agent(
#'     df_veiculos, valid_data, Sys.Date(), schema,
#'     "relatorios/veiculos.html"
#' )
#' }
create_veiculos_agent <- function(
    df_veiculos,
    valid_data,
    data_release,
    schema,
    path
) {
    options(scipen = 9999)

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
            columns = id_veiculo,
            left = 1,
            right = 9999999,
            na_pass = TRUE,
            label = "Min/max válidos de id_veiculo"
        ) |>
        col_vals_between(
            columns = ano_modelo,
            preconditions = function(x) {
                x |> mutate(ano_limite = ano_sinistro + 1)
            },
            right = vars(ano_limite),
            left = 1956,
            na_pass = TRUE,
            label = "min/max de `ano_modelo` - desconsidera os vazios."
        ) |>
        col_vals_between(
            columns = ano_fab,
            right = vars(ano_sinistro),
            left = 1956,
            na_pass = TRUE,
            label = "min/max de `ano_fab` - desconsidera os vazios."
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
            preconditions = function(x) {
                x |> mutate(max_dia = days_in_month(data_sinistro))
            },
            label = "min/max de `dia_sinistro`"
        ) |>
        col_vals_equal(
            columns = ano_mes_sinistro,
            preconditions = function(x) {
                x |> mutate(ano_mes = format(data_sinistro, "%Y/%m"))
            },
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

#' Create schema for 'sinistros' table
#'
#' Defines the expected structure and column types for the 'sinistros' dataset.
#'
#' @return A pointblank `col_schema` object specifying column names and types.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' schema <- create_schema_sinistros()
#' }
create_schema_sinistros <- function() {
    col_schema(
        id_sinistro = "numeric",
        tipo_registro = "character",

        data_sinistro = "Date",
        ano_sinistro = "numeric",
        mes_sinistro = "numeric",
        dia_sinistro = "numeric",
        hora_sinistro = c("hms", "difftime"), # <time> da classe hms
        ano_mes_sinistro = "character",
        dia_da_semana = "character",
        turno = "character",

        logradouro = "character",
        numero_logradouro = "numeric",
        tipo_via = "character",
        tipo_local = "character",
        latitude = "numeric",
        longitude = "numeric",
        cod_ibge = "numeric",
        municipio = "character",
        regiao_administrativa = "character",

        administracao = "character",
        conservacao = "character",
        circunscricao = "character",
        tp_sinistro_primario = "character",

        qtd_pedestre = "numeric",
        qtd_bicicleta = "numeric",
        qtd_motocicleta = "numeric",
        qtd_automovel = "numeric",
        qtd_onibus = "numeric",
        qtd_caminhao = "numeric",
        qtd_veic_outros = "numeric",
        qtd_veic_nao_disponivel = "numeric",

        qtd_gravidade_fatal = "numeric",
        qtd_gravidade_grave = "numeric",
        qtd_gravidade_leve = "numeric",
        qtd_gravidade_ileso = "numeric",
        qtd_gravidade_nao_disponivel = "numeric",

        tp_sinistro_atropelamento = "character",
        tp_sinistro_colisao_frontal = "character",
        tp_sinistro_colisao_traseira = "character",
        tp_sinistro_colisao_lateral = "character",
        tp_sinistro_colisao_transversal = "character",
        tp_sinistro_colisao_outros = "character",

        tp_sinistro_choque = "character",
        tp_sinistro_capotamento = "character",
        tp_sinistro_engavetamento = "character",
        tp_sinistro_tombamento = "character",
        tp_sinistro_outros = "character",
        tp_sinistro_nao_disponivel = "character"
    )
}

#' Validate 'sinistros' dataset using pointblank
#'
#' Runs validation rules on the 'sinistros' dataset from Infosiga using the
#' pointblank package.
#'
#' @param df_sinistros A data frame with the raw 'sinistros' dataset.
#' @param valid_data A named list of valid reference values (e.g. from
#' `create_valid_data()`).
#' @param data_release A Date indicating the current data reference cutoff.
#' @param schema A col_schema object defining expected column types.
#' @param lista_municipios A character vector with valid municipality names.
#' @param path Path to save the validation report (HTML).
#'
#' @return The full path to the validation report file.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' create_sinistros_agent(
#'     df_sinistros, valid_data, Sys.Date(), schema,
#'     lista_municipios, "relatorios/sinistros.html"
#' )
#' }
create_sinistros_agent <- function(
    df_sinistros,
    valid_data,
    data_release,
    schema,
    lista_municipios,
    path
) {
    options(scipen = 9999)

    create_agent(
        tbl = df_sinistros,
        tbl_name = "sinistros",
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
        rows_distinct(
            columns = id_sinistro,
            label = "`id_sinistro` deve ser um valor único"
        ) |>
        col_vals_in_set(
            columns = tipo_registro,
            set = valid_data$lista_tipo_registro,
            label = "Inputs válidos de `tipo_registro`."
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
            preconditions = function(x) {
                x |> mutate(max_dia = days_in_month(data_sinistro))
            },
            label = "min/max de `dia_sinistro`"
        ) |>
        col_vals_between(
            columns = hora_sinistro,
            left = hms::as_hms("00:00:00"),
            right = hms::as_hms("23:59:59"),
            na_pass = TRUE,
            label = "Horários válidos."
        ) |>
        col_vals_equal(
            columns = ano_mes_sinistro,
            preconditions = function(x) {
                x |> mutate(ano_mes = format(data_sinistro, "%Y/%m"))
            },
            value = vars(ano_mes),
            label = "Validação com base em `data_sinistro`"
        ) |>
        col_vals_in_set(
            columns = dia_da_semana,
            set = valid_data$lista_dia_semana,
            label = "Inputs válidos de `dia_semana`"
        ) |>
        col_vals_not_null(
            columns = dia_da_semana,
            label = "`dia_da_semana` não pode ter vazios"
        ) |>
        col_vals_in_set(
            columns = turno,
            set = valid_data$lista_turno,
            label = "Inputs válidos de `turno`"
        ) |>
        col_vals_in_set(
            columns = tipo_via,
            set = valid_data$lista_tipo_via,
            label = "Inputs válidos de `tipo_via`"
        ) |>
        col_vals_in_set(
            columns = tipo_local,
            set = valid_data$lista_tipo_local,
            label = "Inputs válidos de `tipo_local`"
        ) |>
        col_vals_not_null(
            columns = tipo_local,
            label = "`tipo_local` não pode ter vazios"
        ) |>
        col_vals_not_null(
            columns = c(latitude, longitude),
            label = "Verifica coordenadas vazias.",
            segments = tipo_registro ~
                c("SINISTRO FATAL", "SINISTRO NAO FATAL", "NOTIFICACAO")
        ) |>
        col_vals_between(
            columns = latitude,
            left = -25.31,
            right = -19.77,
            na_pass = TRUE,
            label = "Min/max válidos de latitude.",
            segments = tipo_registro ~
                c("SINISTRO FATAL", "SINISTRO NAO FATAL", "NOTIFICACAO")
        ) |>
        col_vals_between(
            columns = longitude,
            left = -53.11,
            right = -44.16,
            na_pass = TRUE,
            label = "Min/max válidos de longitude.",
            segments = tipo_registro ~
                c("SINISTRO FATAL", "SINISTRO NAO FATAL", "NOTIFICACAO")
        ) |>
        col_vals_in_set(
            columns = municipio,
            set = lista_municipios,
            label = "Valores válidos de nome de município."
        ) |>
        col_vals_in_set(
            columns = regiao_administrativa,
            set = valid_data$lista_regiao_administrativa,
            label = "Inputs válidos de região administrativa."
        ) |>
        col_vals_not_null(
            columns = regiao_administrativa,
            label = "`regiao_administrativa` não pode ter vazios"
        ) |>
        col_vals_gte(
            columns = starts_with("qtd_"),
            value = 1,
            label = "Valores não-nulos.",
            na_pass = TRUE
        ) |>
        col_vals_null(
            columns = qtd_gravidade_ileso,
            label = "`qtd_gravidade_ileso` é sempre vazio."
        ) |>
        #col_vals_gte(
        #    columns = starts_with("qtd_gravidade"),
        #    value = 0,
        #    label = "Valores não-negativos.",
        #    na_pass = TRUE
        #) |>
        col_vals_in_set(
            columns = administracao,
            set = valid_data$lista_administracao,
            label = "Inputs válidos de `adminstracao`"
        ) |>
        col_vals_in_set(
            columns = circunscricao,
            set = valid_data$lista_circunscricao,
            label = "Inputs válidos de `circunscricao`"
        ) |>
        col_vals_equal(
            columns = starts_with("tp_sinistro_a"),
            value = "S",
            na_pass = TRUE
        ) |>
        col_vals_equal(
            columns = starts_with("tp_sinistro_c"),
            value = "S",
            na_pass = TRUE
        ) |>
        col_vals_equal(
            columns = starts_with("tp_sinistro_e"),
            value = "S",
            na_pass = TRUE
        ) |>
        col_vals_equal(
            columns = starts_with("tp_sinistro_t"),
            value = "S",
            na_pass = TRUE
        ) |>
        col_vals_equal(
            columns = starts_with("tp_sinistro_o"),
            value = "S",
            na_pass = TRUE
        ) |>
        col_vals_equal(
            columns = starts_with("tp_sinistro_n"),
            value = "S",
            na_pass = TRUE
        ) |>
        interrogate() |>
        get_agent_report(
            title = "Dados abertos Infosiga.SP - Validação da tabela 'sinistros'"
        ) |>
        export_report(
            filename = affix_datetime(path, utc_time = FALSE)
        )
}
