library(targets)

tar_option_set(
    packages = c("tidyverse", "pointblank")
)

tar_source(files = c("main.R"))

list(
    tar_target(path_infosiga, "data/dados_infosiga.zip", format = "file"),
    tar_target(
        df_infosiga,
        map(
            c("pessoas", "veiculos", "sinistros"),
            read_infosiga,
            path = path_infosiga
        )
    )
)
