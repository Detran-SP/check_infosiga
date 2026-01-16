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
            )
        ),
        ui.output_ui("download_section"),
        ui.output_ui("reports_tabs")
    ),
    title="Validação Infosiga-SP"
)

def server(input, output, session):
    @reactive.calc
    def processed_data():
        zip_info = input.zipfile()
        if zip_info is None or len(zip_info) == 0:
            return None
        
        zip_path = zip_info[0]["datapath"]
        
        try:
            df_pessoas = read_infosiga(zip_path, "pessoas")
            df_veiculos = read_infosiga(zip_path, "veiculos")
            df_sinistros = read_infosiga(zip_path, "sinistros")
            
            return {
                "pessoas": df_pessoas,
                "veiculos": df_veiculos,
                "sinistros": df_sinistros
            }
        except Exception as e:
            import traceback
            error_detail = f"{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            return {"error": error_detail}
    
    @reactive.calc
    def validation_reports():
        data = processed_data()
        if data is None:
            return None
        if "error" in data:
            return {"error": data["error"]}
        
        try:
            valid_data = create_valid_data()
            lista_municipios = load_municipios()
            data_release = date(2026, 1, 14)
            
            schema_pessoas = create_schema_pessoas()
            schema_veiculos = create_schema_veiculos()
            schema_sinistros = create_schema_sinistros()
            
            html_pessoas = create_pessoas_agent(
                data["pessoas"],
                valid_data,
                data_release,
                schema_pessoas,
                lista_municipios
            )
            
            html_veiculos = create_veiculos_agent(
                data["veiculos"],
                valid_data,
                data_release,
                schema_veiculos
            )
            
            html_sinistros = create_sinistros_agent(
                data["sinistros"],
                valid_data,
                data_release,
                schema_sinistros,
                lista_municipios
            )
            
            return {
                "pessoas": html_pessoas,
                "veiculos": html_veiculos,
                "sinistros": html_sinistros
            }
        except Exception as e:
            import traceback
            error_detail = f"{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            return {"error": error_detail}

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
            zf.writestr("relatorio_sinistros.html", inject_css(reports["sinistros"]))
            zf.writestr("relatorio_veiculos.html", inject_css(reports["veiculos"]))
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
        
        return ui.navset_tab(
            ui.nav_panel(
                ui.tags.span(
                    ui.tags.i({"class": "fas fa-car-crash"}),
                    " Sinistros"
                ),
                ui.HTML(reports["sinistros"]),
                value="sinistros"
            ),
            ui.nav_panel(
                ui.tags.span(
                    ui.tags.i({"class": "fas fa-car"}),
                    " Veículos"
                ),
                ui.HTML(reports["veiculos"]),
                value="veiculos"
            ),
            ui.nav_panel(
                ui.tags.span(
                    ui.tags.i({"class": "fas fa-users"}),
                    " Pessoas"
                ),
                ui.HTML(reports["pessoas"]),
                value="pessoas"
            ),
            id="report_tabs"
        )

app = App(app_ui, server)
