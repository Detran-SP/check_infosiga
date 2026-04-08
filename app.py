from shiny import App, ui, render, reactive
import tempfile
import zipfile
import os
from datetime import date
from data_processing import read_infosiga
from schemas import create_valid_data, create_schema_pessoas, create_schema_veiculos, create_schema_sinistros, load_municipios
from validation import create_pessoas_agent, create_veiculos_agent, create_sinistros_agent

# CSS customizado
custom_css = """
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@300;400;600;700&display=swap');

body {
    font-family: 'Open Sans', sans-serif;
    background-color: #ffffff;
}

.main-header {
    background: transparent;
    color: #212529;
    padding: 2rem 0;
    margin-bottom: 2rem;
    border-bottom: 2px solid #dee2e6;
}

.main-header h1 {
    font-weight: 700;
    margin-bottom: 0;
    font-size: 2rem;
}

.upload-card {
    background: transparent;
    border: 2px solid #dee2e6;
    border-radius: 10px;
    padding: 2rem;
    margin-bottom: 2rem;
}

.upload-card label {
    font-weight: 600;
    color: #212529;
    margin-bottom: 1rem;
    display: block;
    font-size: 1rem;
}

.upload-card .shiny-input-container {
    width: 100%;
    max-width: 100%;
}

.upload-card .input-group {
    display: flex;
    align-items: stretch;
}

.upload-card .form-control {
    height: 50px;
}

.upload-card .btn-file {
    height: 50px;
    display: flex;
    align-items: center;
    padding: 0 1rem;
}

.nav-tabs {
    border: none;
    gap: 1rem;
    margin-bottom: 1.5rem;
}

.nav-tabs .nav-link {
    border: 2px solid #dee2e6;
    background: transparent;
    border-radius: 10px;
    padding: 0 1.5rem;
    color: #212529;
    font-weight: 600;
    transition: all 0.3s ease;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.75rem;
    height: 50px;
}

.nav-tabs .nav-link:hover {
    border-color: #495057;
}

.nav-tabs .nav-link.active {
    background: transparent;
    color: #212529;
    border-color: #212529;
    border-width: 3px;
}

.nav-tabs .nav-link i {
    font-size: 1.25rem;
}

.tab-content {
    background: transparent;
    border: 2px solid #dee2e6;
    border-radius: 10px;
    padding: 2rem;
}

/* Largura fixa para coluna VALORES do pointblank */
.tab-content td[style*="values"],
.tab-content td:nth-child(5) {
    max-width: 250px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.alert-info {
    background: transparent;
    border: 2px solid #dee2e6;
    border-radius: 10px;
    color: #212529;
    padding: 1.5rem;
    font-weight: 500;
}

.alert-danger {
    background: transparent;
    border: 2px solid #495057;
    border-radius: 10px;
    color: #212529;
    padding: 1.5rem;
}

.download-section {
    margin-bottom: 1.5rem;
}

.btn-download {
    background: transparent;
    border: 2px solid #212529;
    border-radius: 10px;
    color: #212529;
    font-weight: 600;
    padding: 0.75rem 1.5rem;
    transition: all 0.3s ease;
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
}

.btn-download:hover {
    background: #212529;
    color: #ffffff;
}

.spinner-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 3rem;
    gap: 1rem;
}

.spinner-border {
    width: 3rem;
    height: 3rem;
    border-width: 0.3rem;
    border-color: #212529;
    border-right-color: transparent;
}

.text-primary {
    color: #212529 !important;
}

.btn-process {
    background: #212529;
    border: 2px solid #212529;
    border-radius: 10px;
    color: #ffffff;
    font-weight: 600;
    padding: 0.75rem 2rem;
    transition: all 0.3s ease;
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 1rem;
    margin-top: 1rem;
}

.btn-process:hover {
    background: #495057;
    border-color: #495057;
    color: #ffffff;
}

.btn-process:disabled {
    background: #dee2e6;
    border-color: #dee2e6;
    color: #6c757d;
    cursor: not-allowed;
}

.status-message {
    background: transparent;
    border: 2px solid #212529;
    border-radius: 10px;
    padding: 1rem 1.5rem;
    margin-top: 1rem;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 0.75rem;
}

.status-message.processing {
    border-color: #0d6efd;
    color: #0d6efd;
}

.status-message.success {
    border-color: #198754;
    color: #198754;
}

.status-message.error {
    border-color: #dc3545;
    color: #dc3545;
}

.table-selection {
    margin-top: 1.5rem;
    padding-top: 1.5rem;
    border-top: 2px solid #dee2e6;
}

.table-selection label {
    font-weight: 600;
    color: #212529;
    margin-bottom: 0.75rem;
    display: block;
}

.table-selection select {
    width: 100%;
    border: 2px solid #dee2e6;
    border-radius: 10px;
    padding: 0.5rem;
    font-family: 'Open Sans', sans-serif;
    font-size: 1rem;
    font-weight: 500;
    color: #212529;
    background: transparent;
    cursor: pointer;
    outline: none;
}

.table-selection select:focus {
    border-color: #212529;
}
"""

app_ui = ui.page_bootstrap(
    ui.tags.head(
        ui.tags.style(custom_css),
        ui.tags.link(
            rel="stylesheet",
            href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"
        )
    ),
    ui.div(
        {"class": "main-header"},
        ui.div(
            {"class": "container"},
            ui.h1("Validação dos dados abertos do Infosiga")
        )
    ),
    ui.div(
        {"class": "container"},
        ui.div(
            {"class": "upload-card"},
            ui.input_file(
                "zipfile",
                ui.tags.span(
                    ui.tags.i({"class": "fas fa-file-zipper me-2"}),
                    "Arquivo ZIP dos Dados"
                ),
                accept=[".zip"],
                multiple=False,
                button_label="Selecionar Arquivo",
                placeholder="Nenhum arquivo selecionado"
            ),
            ui.output_ui("table_selection_ui"),
            ui.output_ui("process_button_ui"),
            ui.output_ui("status_message")
        ),
        ui.output_ui("download_section"),
        ui.output_ui("reports_tabs")
    ),
    title="Validação Infosiga-SP"
)

def server(input, output, session):
    # Estado para armazenar dados processados
    processed_data_store = reactive.value(None)
    validation_reports_store = reactive.value(None)
    processing_status = reactive.value("")

    @reactive.effect
    @reactive.event(input.zipfile)
    def reset_on_file_change():
        """Reset status when a new file is uploaded"""
        processing_status.set("")
        validation_reports_store.set(None)
        processed_data_store.set(None)

    @output
    @render.ui
    def table_selection_ui():
        zip_info = input.zipfile()
        if zip_info is None or len(zip_info) == 0:
            return None

        return ui.div(
            {"class": "table-selection"},
            ui.input_select(
                "table_select",
                "Selecione a tabela para validar:",
                choices={
                    "sinistros": "Sinistros",
                    "veiculos": "Veículos",
                    "pessoas": "Pessoas"
                },
                selected="sinistros",
                multiple=False,
                selectize=False,
                size="3"
            )
        )

    @output
    @render.ui
    def process_button_ui():
        zip_info = input.zipfile()
        if zip_info is None or len(zip_info) == 0:
            return None

        status = processing_status()
        is_processing = status.startswith("processing")

        if is_processing:
            return ui.tags.button(
                ui.tags.span(
                    ui.div(
                        {"class": "spinner-border spinner-border-sm me-2", "role": "status"},
                        ui.tags.span({"class": "visually-hidden"}, "Processando...")
                    ),
                    "Processando..."
                ),
                class_="btn-process",
                disabled=True
            )

        return ui.input_action_button(
            "process_btn",
            ui.tags.span(
                ui.tags.i({"class": "fas fa-play me-2"}),
                "Iniciar Validação"
            ),
            class_="btn-process"
        )

    @output
    @render.ui
    def status_message():
        status = processing_status()
        if not status:
            return None

        if status.startswith("processing"):
            parts = status.split(":", 1)
            message = parts[1] if len(parts) > 1 else "Processando dados e gerando relatórios de validação..."
            return ui.div(
                {"class": "status-message processing"},
                ui.div(
                    {"class": "spinner-border spinner-border-sm", "role": "status"},
                    ui.tags.span({"class": "visually-hidden"}, "Processando...")
                ),
                ui.tags.span(message)
            )
        elif status == "success":
            return ui.div(
                {"class": "status-message success"},
                ui.tags.i({"class": "fas fa-check-circle"}),
                ui.tags.span("Validação concluída com sucesso!")
            )
        elif status.startswith("error:"):
            error_msg = status.replace("error:", "")
            return ui.div(
                {"class": "status-message error"},
                ui.tags.i({"class": "fas fa-exclamation-circle"}),
                ui.tags.span(f"Erro: {error_msg}")
            )
        return None

    @reactive.effect
    @reactive.event(input.process_btn)
    def process_data():
        import traceback
        import sys

        zip_info = input.zipfile()
        if zip_info is None or len(zip_info) == 0:
            print("ERRO: Nenhum arquivo ZIP foi carregado", file=sys.stderr)
            return

        try:
            processing_status.set("processing:Iniciando processamento...")
            print("Iniciando processamento...", file=sys.stderr)
            zip_path = zip_info[0]["datapath"]
            print(f"Arquivo ZIP: {zip_path}", file=sys.stderr)

            # Verificar qual tabela processar
            selected_table = input.table_select()
            process_pessoas = selected_table == "pessoas"
            process_veiculos = selected_table == "veiculos"
            process_sinistros = selected_table == "sinistros"

            print(f"Tabela selecionada: {selected_table}", file=sys.stderr)

            # Leitura dos dados (apenas tabelas selecionadas)
            data = {}

            if process_pessoas:
                processing_status.set("processing:Lendo dados de pessoas...")
                print("Lendo dados de pessoas...", file=sys.stderr)
                df_pessoas = read_infosiga(zip_path, "pessoas")
                print(f"Pessoas: {len(df_pessoas)} registros", file=sys.stderr)
                data["pessoas"] = df_pessoas

            if process_veiculos:
                processing_status.set("processing:Lendo dados de veículos...")
                print("Lendo dados de veículos...", file=sys.stderr)
                df_veiculos = read_infosiga(zip_path, "veiculos")
                print(f"Veículos: {len(df_veiculos)} registros", file=sys.stderr)
                data["veiculos"] = df_veiculos

            if process_sinistros:
                processing_status.set("processing:Lendo dados de sinistros...")
                print("Lendo dados de sinistros...", file=sys.stderr)
                df_sinistros = read_infosiga(zip_path, "sinistros")
                print(f"Sinistros: {len(df_sinistros)} registros", file=sys.stderr)
                data["sinistros"] = df_sinistros

            processed_data_store.set(data)

            # Geração dos relatórios de validação
            processing_status.set("processing:Carregando configurações de validação...")
            print("Carregando configurações de validação...", file=sys.stderr)
            valid_data = create_valid_data()
            lista_municipios = load_municipios()
            data_release = date(2026, 1, 14)

            reports = {}

            if process_pessoas:
                schema_pessoas = create_schema_pessoas()
                processing_status.set("processing:Validando dados de pessoas...")
                print("Gerando relatório de pessoas...", file=sys.stderr)
                try:
                    html_pessoas = create_pessoas_agent(
                        data["pessoas"],
                        valid_data,
                        data_release,
                        schema_pessoas,
                        lista_municipios
                    )
                    print("Relatório de pessoas concluído", file=sys.stderr)
                    reports["pessoas"] = html_pessoas
                except Exception as e:
                    print(f"ERRO no relatório de pessoas: {str(e)}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    raise

            if process_veiculos:
                schema_veiculos = create_schema_veiculos()
                processing_status.set("processing:Validando dados de veículos...")
                print("Gerando relatório de veículos...", file=sys.stderr)
                try:
                    html_veiculos = create_veiculos_agent(
                        data["veiculos"],
                        valid_data,
                        data_release,
                        schema_veiculos
                    )
                    print("Relatório de veículos concluído", file=sys.stderr)
                    reports["veiculos"] = html_veiculos
                except Exception as e:
                    print(f"ERRO no relatório de veículos: {str(e)}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    raise

            if process_sinistros:
                schema_sinistros = create_schema_sinistros()
                processing_status.set("processing:Validando dados de sinistros...")
                print("Gerando relatório de sinistros...", file=sys.stderr)
                try:
                    html_sinistros = create_sinistros_agent(
                        data["sinistros"],
                        valid_data,
                        data_release,
                        schema_sinistros,
                        lista_municipios
                    )
                    print("Relatório de sinistros concluído", file=sys.stderr)
                    reports["sinistros"] = html_sinistros
                except Exception as e:
                    print(f"ERRO no relatório de sinistros: {str(e)}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    raise
            validation_reports_store.set(reports)
            processing_status.set("success")
            print("Processamento concluído com sucesso!", file=sys.stderr)

        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            print(f"ERRO durante processamento: {error_msg}", file=sys.stderr)
            print(f"Traceback:\n{error_trace}", file=sys.stderr)

            error_detail = f"{error_msg}\n\nTraceback:\n{error_trace}"
            validation_reports_store.set({"error": error_detail})
            processing_status.set(f"error:{error_msg}")

            # Garantir que o erro não cause crash do app
            return

    @reactive.calc
    def validation_reports():
        return validation_reports_store()

    @output
    @render.ui
    def download_section():
        reports = validation_reports()
        if reports is None or "error" in reports:
            return None

        return ui.div(
            {"class": "download-section"},
            ui.download_button(
                "download_reports",
                ui.tags.span(
                    ui.tags.i({"class": "fas fa-download me-2"}),
                    "Baixar Relatórios (ZIP)"
                ),
                class_="btn-download"
            )
        )

    @render.download(filename="relatorios_validacao.zip")
    def download_reports():
        reports = validation_reports()
        if reports is None or "error" in reports:
            return

        # CSS para limitar largura da coluna VALORES
        report_css = """
        <style>
        td:nth-child(5) {
            max-width: 250px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        </style>
        """

        def inject_css(html):
            if "</head>" in html:
                return html.replace("</head>", report_css + "</head>")
            elif "<body" in html:
                return html.replace("<body", report_css + "<body")
            return report_css + html

        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        temp_zip.close()

        with zipfile.ZipFile(temp_zip.name, "w", zipfile.ZIP_DEFLATED) as zf:
            if "sinistros" in reports:
                zf.writestr("relatorio_sinistros.html", inject_css(reports["sinistros"]))
            if "veiculos" in reports:
                zf.writestr("relatorio_veiculos.html", inject_css(reports["veiculos"]))
            if "pessoas" in reports:
                zf.writestr("relatorio_pessoas.html", inject_css(reports["pessoas"]))

        with open(temp_zip.name, "rb") as f:
            yield f.read()

        os.unlink(temp_zip.name)

    @output
    @render.ui
    def reports_tabs():
        reports = validation_reports()

        if reports is None:
            return ui.div(
                {"class": "alert alert-info"},
                ui.tags.i({"class": "fas fa-info-circle me-2"}),
                "Aguardando upload do arquivo ZIP com os dados do Infosiga..."
            )

        if "error" in reports:
            return ui.div(
                {"class": "alert alert-danger"},
                ui.h5(
                    ui.tags.i({"class": "fas fa-exclamation-triangle me-2"}),
                    "Erro ao processar dados"
                ),
                ui.tags.pre(
                    str(reports["error"]),
                    style="white-space: pre-wrap; word-wrap: break-word;"
                )
            )

        # Criar tabs apenas para as tabelas processadas
        tabs = []

        if "sinistros" in reports:
            tabs.append(
                ui.nav_panel(
                    ui.tags.span(
                        ui.tags.i({"class": "fas fa-car-crash"}),
                        " Sinistros"
                    ),
                    ui.HTML(reports["sinistros"]),
                    value="sinistros"
                )
            )

        if "veiculos" in reports:
            tabs.append(
                ui.nav_panel(
                    ui.tags.span(
                        ui.tags.i({"class": "fas fa-car"}),
                        " Veículos"
                    ),
                    ui.HTML(reports["veiculos"]),
                    value="veiculos"
                )
            )

        if "pessoas" in reports:
            tabs.append(
                ui.nav_panel(
                    ui.tags.span(
                        ui.tags.i({"class": "fas fa-users"}),
                        " Pessoas"
                    ),
                    ui.HTML(reports["pessoas"]),
                    value="pessoas"
                )
            )

        if not tabs:
            return ui.div(
                {"class": "alert alert-info"},
                ui.tags.i({"class": "fas fa-info-circle me-2"}),
                "Nenhuma tabela foi selecionada para validação. Selecione ao menos uma tabela e clique em 'Iniciar Validação'."
            )

        return ui.navset_tab(*tabs, id="report_tabs")

app = App(app_ui, server)
