library(targets)

tar_option_set(
    packages = c("tidyverse", "pointblank")
)

tar_source(files = c("main.R"))

list(
    tar_target(data_release, as.Date("2025-06-16")),
    tar_target(path_infosiga, "data/dados_infosiga.zip", format = "file"),
    tar_target(
        df_infosiga,
        map(
            c("pessoas", "veiculos", "sinistros"),
            read_infosiga,
            path = path_infosiga
        )
    ),
    tar_target(valid_data, create_valid_data()),
    tar_target(lista_municipios, readRDS("data/municipios.rds")),
    tar_target(schema_pessoas, create_schema_pessoas()),
    tar_target(schema_veiculos, create_schema_veiculos()),
    tar_target(schema_sinistros, create_schema_sinistros()),
    tar_target(
        agent_pessoas,
        create_pessoas_agent(
            df_infosiga[[1]],
            valid_data,
            data_release,
            schema_pessoas,
            lista_municipios,
            "report/pessoas.html"
        )
    ),
    tar_target(
        agent_veiculos,
        create_veiculos_agent(
            df_infosiga[[2]],
            valid_data,
            data_release,
            schema_veiculos,
            "report/veiculos.html"
        )
    ),
    tar_target(
        agent_sinistros,
        create_sinistros_agent(
            df_infosiga[[3]],
            valid_data,
            data_release,
            schema_sinistros,
            lista_municipios,
            "report/sinistros.html"
        )
    )
)
