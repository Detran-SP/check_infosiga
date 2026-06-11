"""Exportação e comparação mês a mês dos resultados de validação (issue #15).

O app exporta os resultados de cada tabela em JSON (`wrap_report`) e permite
reimportar um conjunto desses JSONs para montar uma tabela comparativa
(`build_comparison_html`), mostrando a evolução do nº de falhas de cada checagem
ao longo dos meses.
"""

import json
from datetime import date
from typing import Dict, List

SCHEMA_VERSION = 1

# Rótulos amigáveis por tabela e ordem de exibição na comparação.
TABLE_LABELS = {
    "sinistros": "Sinistros",
    "veiculos": "Veículos",
    "pessoas": "Pessoas",
}


def wrap_report(table: str, data_release: date, json_str: str, app_version: str) -> Dict:
    """Empacota o JSON cru do pointblank com metadados de identificação.

    `json_str` é a string retornada por `pb.Validate.get_json_report()` (um array
    de etapas). O resultado é um dicionário serializável pronto para gravar em
    disco e reimportar depois.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "table": table,
        "data_release": data_release.isoformat(),
        "app_version": app_version,
        "steps": json.loads(json_str),
    }


def parse_report(text: str) -> Dict:
    """Lê e valida um JSON exportado anteriormente.

    Levanta `ValueError` com mensagem amigável quando o conteúdo não é um
    relatório válido (JSON malformado, versão de schema desconhecida ou campos
    obrigatórios ausentes).
    """
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Arquivo não é um JSON válido: {e}")

    if not isinstance(data, dict):
        raise ValueError("Arquivo JSON não tem o formato esperado de relatório.")

    if data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"Versão de schema incompatível (esperado {SCHEMA_VERSION}, "
            f"encontrado {data.get('schema_version')!r})."
        )

    for field in ("table", "data_release", "steps"):
        if field not in data:
            raise ValueError(f"Campo obrigatório ausente no JSON: '{field}'.")

    if data["table"] not in TABLE_LABELS:
        raise ValueError(f"Tabela desconhecida no JSON: {data['table']!r}.")

    return data


def _month_label(data_release: str) -> str:
    """Converte 'YYYY-MM-DD' em rótulo de mês 'YYYY/MM'."""
    d = date.fromisoformat(data_release)
    return f"{d.year}/{d.month:02d}"


def _step_key(step: Dict) -> tuple:
    """Chave estável que identifica uma checagem entre meses.

    Usa a descrição (`brief`) somada ao valor do segmento, quando houver, para
    não colidir em checagens segmentadas (que repetem o mesmo `brief`).
    """
    brief = step.get("brief") or f"{step.get('assertion_type')} ({step.get('column')})"
    segments = step.get("segments")
    seg = tuple(segments) if isinstance(segments, list) else None
    return (brief, seg)


def _step_label(step: Dict) -> str:
    # O número exibido na comparação é a contagem de falhas (inputs inválidos);
    # as checagens da família "válidos" são reescritas para refletir isso (issue #24).
    brief = (
        step.get("brief") or f"{step.get('assertion_type')} ({step.get('column')})"
    ).replace("válidos", "inválidos")
    segments = step.get("segments")
    if isinstance(segments, list) and len(segments) == 2:
        return f"{brief} [segmento {segments[0]} = {segments[1]}]"
    return brief


def _pct1(step: Dict):
    """Percentual de falhas arredondado a 1 casa, ou None se indisponível."""
    f = step.get("f_failed")
    return round(f * 100, 1) if isinstance(f, (int, float)) else None


def _format_cell(step: Dict) -> str:
    """Formata uma célula: 'N (P%)' de falhas, ou '0' quando não há falhas."""
    n_failed = step.get("n_failed")
    f_failed = step.get("f_failed")
    if n_failed is None:
        return "—"
    if not n_failed:
        return "0"
    pct = f"{f_failed * 100:.1f}%" if isinstance(f_failed, (int, float)) else "—"
    return f"{n_failed} ({pct})"


def build_comparison_html(reports: List[Dict]) -> str:
    """Monta o HTML comparativo agrupado por tabela.

    Cada `reports[i]` é um dicionário no formato de `wrap_report`/`parse_report`.
    Pode haver vários relatórios da mesma tabela (um por mês). Para cada tabela,
    gera uma tabela HTML com uma coluna por mês e uma linha por checagem,
    destacando aumentos no nº de falhas em relação ao mês anterior.
    """
    # Agrupa por tabela, preservando a ordem canônica de TABLE_LABELS.
    by_table: Dict[str, List[Dict]] = {}
    for rep in reports:
        by_table.setdefault(rep["table"], []).append(rep)

    sections = []
    for table in TABLE_LABELS:
        if table not in by_table:
            continue
        sections.append(_build_table_section(table, by_table[table]))

    return "\n".join(sections)


def _build_table_section(table: str, reports: List[Dict]) -> str:
    # Ordena os meses cronologicamente; ignora múltiplos relatórios do mesmo mês
    # mantendo o último encontrado.
    reps_by_month: Dict[str, Dict] = {}
    for rep in reports:
        reps_by_month[rep["data_release"]] = rep
    months = sorted(reps_by_month.keys())

    # Ordem das linhas: definida pelas checagens do mês mais recente; checagens
    # que só aparecem em meses anteriores são acrescentadas ao final.
    row_order: List[tuple] = []
    row_labels: Dict[tuple, str] = {}
    row_ids: Dict[tuple, object] = {}
    seen = set()
    for month in reversed(months):
        for step in reps_by_month[month]["steps"]:
            key = _step_key(step)
            if key not in seen:
                seen.add(key)
                row_order.append(key)
                row_labels[key] = _step_label(step)
                row_ids[key] = step.get("i")

    # Mapa: mês -> {chave -> step}
    steps_by_month: Dict[str, Dict[tuple, Dict]] = {
        month: {_step_key(s): s for s in reps_by_month[month]["steps"]}
        for month in months
    }

    # Cabeçalho
    header_cells = "".join(f"<th>{_month_label(m)}</th>" for m in months)
    head = f"<tr><th class='id-col'>Nº</th><th class='check-col'>Checagem</th>{header_cells}</tr>"

    # Linhas
    body_rows = []
    for key in row_order:
        cells = []
        prev_pct = None
        for month in months:
            step = steps_by_month[month].get(key)
            if step is None:
                cells.append("<td class='cmp-missing'></td>")
                prev_pct = None
                continue
            pct = _pct1(step)
            cls = ""
            if prev_pct is not None and pct is not None:
                if pct > prev_pct:
                    cls = " class='cmp-worse'"
                elif pct < prev_pct:
                    cls = " class='cmp-better'"
            cells.append(f"<td{cls}>{_format_cell(step)}</td>")
            prev_pct = pct
        label = row_labels[key]
        step_id = row_ids[key]
        id_txt = "" if step_id is None else step_id
        body_rows.append(
            f"<tr><td class='id-col'>{id_txt}</td>"
            f"<td class='check-col'>{label}</td>{''.join(cells)}</tr>"
        )

    return f"""
<div class="cmp-section">
  <h3 class="cmp-title">{TABLE_LABELS[table]}</h3>
  <div class="cmp-table-wrap">
    <table class="cmp-table">
      <thead>{head}</thead>
      <tbody>{''.join(body_rows)}</tbody>
    </table>
  </div>
</div>
"""


# CSS da seção de comparação, injetado uma vez no app.
COMPARISON_CSS = """
.cmp-section { margin-bottom: 2rem; }
.cmp-title { font-weight: 700; color: #212529; margin-bottom: 0.75rem; }
.cmp-table-wrap { overflow-x: auto; }
.cmp-table {
    border-collapse: collapse;
    width: 100%;
    font-family: 'Open Sans', sans-serif;
    font-size: 0.9rem;
}
.cmp-table th, .cmp-table td {
    border: 1px solid #dee2e6;
    padding: 0.5rem 0.75rem;
    text-align: center;
    white-space: nowrap;
}
.cmp-table th { background: #f8f9fa; font-weight: 600; color: #212529; }
.cmp-table td.id-col, .cmp-table th.id-col {
    width: 3rem;
    color: #6c757d;
    font-variant-numeric: tabular-nums;
}
.cmp-table td.check-col, .cmp-table th.check-col {
    text-align: left;
    white-space: normal;
    min-width: 280px;
    max-width: 480px;
}
.cmp-table td.cmp-worse { background: #f8d7da; color: #842029; font-weight: 600; }
.cmp-table td.cmp-better { background: #d1e7dd; color: #0f5132; }
.cmp-table td.cmp-missing { background: #f1f3f5; }
"""
