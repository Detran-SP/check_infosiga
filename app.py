from shiny import App, ui, render, reactive
import tempfile
import zipfile
import os
from datetime import date
from data_processing import read_infosiga
from schemas import create_valid_data, create_schema_pessoas, create_schema_veiculos, create_schema_sinistros, load_municipios
from validation import create_pessoas_agent, create_veiculos_agent, create_sinistros_agent

app_ui = ui.page_fluid(
    ui.h2("Validação de Dados Infosiga-SP"),
    ui.p("Faça o upload do arquivo ZIP contendo os dados do Infosiga para validar a qualidade dos dados."),
    ui.input_file(
        "zipfile",
        "Escolha o arquivo ZIP",
        accept=[".zip"],
        multiple=False
    ),
    ui.output_ui("reports_tabs")
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
    def reports_tabs():
        reports = validation_reports()
        
        if reports is None:
            return ui.p("Aguardando upload do arquivo ZIP...")
        
        if "error" in reports:
            return ui.div(
                ui.h4("Erro ao processar dados"),
                ui.p(str(reports["error"])),
                style="color: red;"
            )
        
        return ui.navset_tab(
            ui.nav_panel(
                "Sinistros",
                ui.HTML(reports["sinistros"])
            ),
            ui.nav_panel(
                "Veículos",
                ui.HTML(reports["veiculos"])
            ),
            ui.nav_panel(
                "Pessoas",
                ui.HTML(reports["pessoas"])
            )
        )

app = App(app_ui, server)
