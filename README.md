# check_infosiga

Validação automatizada da qualidade dos dados do Infosiga-SP utilizando o pacote `pointblank` e o framework web `Shiny` para Python.

## Visão Geral

Este repositório automatiza a leitura, verificação e geração de relatórios de qualidade dos dados do Infosiga-SP (sinistros, pessoas e veículos). O processo inclui:

- Upload de arquivo ZIP com os dados brutos do Infosiga através de interface web;
- Extração e leitura automática dos arquivos CSV;
- Validação de esquemas esperados por tipo de dado (`pessoas`, `veiculos`, `sinistros`);
- Geração de relatórios em HTML interativos com alertas de conformidade;
- Visualização dos relatórios em abas separadas na interface web.

## Estrutura do Projeto

```
check_infosiga/
├── pyproject.toml          # Configuração do projeto e dependências
├── uv.lock                 # Lockfile do uv
├── app.py                  # Aplicação Shiny principal
├── data_processing.py      # Funções de leitura e processamento de dados
├── validation.py           # Funções de validação com pointblank
├── schemas.py              # Definições de schemas e dados válidos
├── data/
│   ├── dados_infosiga.zip  # Arquivo ZIP de entrada (exemplo)
│   └── municipios.json     # Lista de municípios válidos
└── README.md
```

## Requisitos

- Python 3.13
- `uv` (gerenciador de pacotes Python)

## Instalação

1. Instale o `uv` se ainda não tiver:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Clone o repositório e entre no diretório:
```bash
cd check_infosiga
```

3. Instale as dependências:
```bash
uv sync
```

## Execução

1. Execute a aplicação Shiny:
```bash
uv run shiny run app.py
```

2. Abra o navegador no endereço indicado (geralmente `http://127.0.0.1:8000`)

3. Faça o upload do arquivo ZIP contendo os dados do Infosiga através da interface web

4. Os relatórios de validação serão exibidos automaticamente em três abas:
   - **Sinistros**: Validação da tabela de sinistros
   - **Veículos**: Validação da tabela de veículos
   - **Pessoas**: Validação da tabela de pessoas

## Relatórios Gerados

O processo gera 3 relatórios HTML interativos com o diagnóstico de qualidade dos dados:

- Relatório de **sinistros**: Validações de tipos, valores, datas, coordenadas, etc.
- Relatório de **veículos**: Validações de tipos, anos, datas, etc.
- Relatório de **pessoas**: Validações de tipos, idades, datas, gravidade de lesões, etc.

Cada relatório é exibido em uma aba separada na interface web e mostra:
- Status de cada validação (passou/falhou)
- Número de linhas que passaram/falharam em cada teste
- Detalhes sobre os problemas encontrados

## Desenvolvimento

O projeto utiliza:
- **Shiny para Python**: Framework web reativo para criar a interface
- **pointblank**: Biblioteca de validação de dados
- **pandas**: Manipulação e análise de dados
- **polars**: Dependência requerida pelo pointblank (não usado diretamente)
- **uv**: Gerenciador de pacotes e ambiente virtual

## Contato

[estudos.transito@detran.sp.gov.br](mailto:estudos.transito@detran.sp.gov.br)
