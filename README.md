# check_infosiga

Verificação automatizada da qualidade dos dados do Infosiga-SP utilizando o pacote [`pointblank`](https://rich-iannone.github.io/pointblank/) e o pipeline `{targets}`.

## Visão Geral

Este repositório automatiza a leitura, verificação e geração de relatórios de qualidade dos dados do Infosiga-SP (sinistros, pessoas e veículos). O processo inclui:

- Extração de arquivos ZIP com os dados brutos do Infosiga;

- Validação de esquemas esperados por tipo de dado (`pessoas`, `veiculos`, `sinistros`);

- Geração de relatórios em HTML com alertas de conformidade.

## 🛠️ Estrutura do Projeto

```
check_infosiga/
├── _targets.R # Pipeline do projeto usando {targets}
├── main.R # Funções principais (leitura, validação)
├── data/
│ └── municipios.rds # Lista de municípios válidos
├── report/ # Saída esperada com relatórios HTML
├── renv/ # Ambiente isolado com dependências
├── renv.lock # Lockfile de pacotes
├── .Rprofile # Configuração para reprodutibilidade
└── .gitignore
```

## Execução

1. Instale o pacote `{renv}`:

```r
install.packages("renv")
```

2. Inicialize o ambiente:

```r
renv::restore()
```

3. Execute o pipeline:

```r
targets::tar_make()
```

4. Os relatórios serão gerados na pasta `report/`.

## Entradas Esperadas

- `data/dados_infosiga.zip`: arquivo ZIP contendo os CSVs de dados abertos do Infosiga.SP.

## Relatórios Gerados

O processo gera 3 arquivos HTML com o diagnóstico de qualidade dos dados:

- `report/pessoas.html`
- `report/veiculos.html`
- `report/sinistros.html`

## Contato

[estudos.transito@detran.sp.gov.br](mailto:estudos.transito@detran.sp.gov.br)