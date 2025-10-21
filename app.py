#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard de Métricas de Emails — versión optimizada con DuckDB 🦆
- Lee particiones Parquet remotas directamente desde Hugging Face Datasets.
- Filtra por fecha y agencia sin cargar toda la data a memoria.
"""

import os
import re
import duckdb
from datetime import date
import pandas as pd

import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

# ================== Config ==================
DATASET_URL = os.getenv(
    "PARQUET_REMOTE_URL",
    "https://huggingface.co/datasets/mikegrhub/email_metrics_data/resolve/main/year=*/month=*/*.parquet"
)
DATE_COLUMN = os.getenv("DATE_COLUMN", "Fecha_de_creacion")
EMAIL_COLUMN = os.getenv("EMAIL_COLUMN", "Email")

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# ================== DuckDB Helper ==================
def query_duckdb(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET enable_progress_bar = false;")
    return con.execute(sql).fetchdf()

def load_df_full(start=None, end=None):
    print(f"🦆 Cargando datos desde DuckDB remoto... rango {start} → {end}")

    where = []
    if start:
        where.append(f"{DATE_COLUMN} >= '{start}'")
    if end:
        where.append(f"{DATE_COLUMN} <= '{end}'")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
        SELECT Email, agency, Destination, condactivacion, Localizador, {DATE_COLUMN}
        FROM read_parquet('{DATASET_URL}')
        {where_clause}
    """
    df = query_duckdb(sql)
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors="coerce").dt.date
    return df

# ================== Archivos agregados (ligeros) ==================
print("📥 Cargando métricas agregadas...")
metrics_daily = pd.read_parquet("data/metrics_daily.parquet")
metrics_daily["metric_date"] = pd.to_datetime(metrics_daily["metric_date"]).dt.date
top_domains_daily = pd.read_parquet("data/metrics_top_domains_daily.parquet")
repeated_hist = pd.read_parquet("data/metrics_repeated_emails.parquet")

date_min = metrics_daily["metric_date"].min()
date_max = metrics_daily["metric_date"].max()

# ================== UI Helpers ==================
def kpi_card(value, title, color="dark"):
    bg_colors = {
        "dark": "bg-dark",
        "blue": "bg-primary",
        "green": "bg-success",
        "orange": "bg-warning",
        "red": "bg-danger",
    }
    title_class = "text-dark fw-semibold d-block mb-1 text-center"
    value_class = "text-light fw-bold mb-0 text-center"
    if color == "orange":
        value_class = "text-dark fw-bold mb-0 text-center"

    return dbc.Card(
        dbc.CardBody([
            html.Small(title, className=title_class),
            html.H3(f"{value:,}", className=value_class),
        ]),
        className=f"{bg_colors.get(color, 'bg-dark')} text-center border-0 rounded-3 shadow-sm"
    )

def bar_percentages(total, with_email, valid, uniques, sendable):
    cats = [
        "Total (100%)",
        "Con email (% total)",
        "Válidos (% con email)",
        "Únicos (% total)",
        "Enviables (% total)",
    ]
    vals = [
        100 if total else 0,
        (with_email / total * 100) if total else 0,
        (valid / with_email * 100) if with_email else 0,
        (uniques / total * 100) if total else 0,
        (sendable / total * 100) if total else 0,
    ]
    fig = px.bar(x=cats, y=vals, title="Volumen del día (porcentajes)")
    fig.update_traces(text=[f"{v:.1f}%" for v in vals], textposition="outside")
    fig.update_layout(yaxis_title=None, xaxis_title=None, uniformtext_minsize=10, uniformtext_mode="hide")
    return fig

def dq_pie(ok, duplicate, empty, invalid):
    names = ["ok", "duplicate", "empty", "invalid_format"]
    vals  = [ok, duplicate, empty, invalid]
    fig = px.pie(names=names, values=vals, title="Razones DQ (del rango)")
    return fig

# ================== App ==================
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
server = app.server
app.title = "📊 Dashboard de Métricas de Emails"

# ================== Layout ==================
filter_layout = dbc.Container([
    dbc.Row([
        dbc.Col([html.Label("Agency"), dcc.Input(id="agency-filter", placeholder="Ej: BDR_ROYALTON", type="text", style={"width": "100%"})], md=3),
        dbc.Col([html.Label("Destination"), dcc.Input(id="destination-filter", placeholder="Ej: Cancún", type="text", style={"width": "100%"})], md=3),
        dbc.Col([html.Label("Condactivacion"), dcc.Input(id="cond-filter", placeholder="Ej: prearrival", type="text", style={"width": "100%"})], md=3),
        dbc.Col([html.Label("Localizador"), dcc.Input(id="localizador-filter", placeholder="Ej: ZBRVQ6", type="text", style={"width": "100%"})], md=3),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col([
            html.Label("Rango de fechas"),
            dcc.DatePickerRange(
                id="date-range",
                min_date_allowed=date_min, max_date_allowed=date_max,
                start_date=date_min, end_date=date_max,
                display_format="YYYY-MM-DD"
            )
        ], md=4),
        dbc.Col([html.Br(), dbc.Button("Aplicar filtros", id="apply-filters", color="primary", className="w-100")], md=2),
        dbc.Col([], md=6),
    ]),

    html.Hr(),

    dbc.Row([
        dbc.Col(dbc.Placeholder(id="kpi-total", style={"height": 90}, color="secondary"), md=2),
        dbc.Col(dbc.Placeholder(id="kpi-with-email", style={"height": 90}, color="secondary"), md=2),
        dbc.Col(dbc.Placeholder(id="kpi-valid", style={"height": 90}, color="secondary"), md=3),
        dbc.Col(dbc.Placeholder(id="kpi-sendable", style={"height": 90}, color="secondary"), md=3),
        dbc.Col(dbc.Placeholder(id="kpi-unique-sendable", style={"height": 90}, color="secondary"), md=2),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col(dcc.Graph(id="filtered-perc-bar"), md=6),
        dbc.Col(dcc.Graph(id="filtered-dq-pie"), md=6),
    ], className="mb-3"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="filtered-top-domains"), md=6),
        dbc.Col(dcc.Graph(id="filtered-duplicated-emails"), md=6),
    ]),
], fluid=True)

historico_layout = dbc.Container([
    html.Hr(),
    dbc.Row([
        dbc.Col(kpi_card(int(metrics_daily["total_rows"].sum()), "Registros (histórico)", "blue"), md=2),
        dbc.Col(kpi_card(int(metrics_daily["with_email"].sum()), "Con email (suma)", "green"), md=2),
        dbc.Col(kpi_card(int(metrics_daily["valid_emails"].sum()), "Formato válido (suma)", "orange"), md=3),
        dbc.Col(kpi_card(int(metrics_daily["sendable_emails"].sum()), "Enviables (suma)", "green"), md=3),
        dbc.Col(kpi_card(int(metrics_daily["unique_valid_emails"].sum()), "Únicos válidos (suma)", "dark"), md=2),
    ], className="mb-4"),

    dcc.Graph(
        figure=px.line(
            metrics_daily.melt(id_vars=["metric_date"], var_name="variable", value_name="value"),
            x="metric_date", y="value", color="variable",
            title="Evolución de métricas diarias"
        )
    ),

    html.Br(),

    dcc.Graph(
        figure=px.bar(
            top_domains_daily.groupby("domain", as_index=False)["cnt"].sum()
                .sort_values("cnt", ascending=False).head(10),
            x="domain", y="cnt", title="Top 10 dominios más frecuentes (histórico)"
        )
    ),

    html.Br(),

    html.H5("Emails repetidos (histórico total)"),
    dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in ["email", "occurrences", "first_seen", "last_seen"]],
        data=repeated_hist.sort_values("occurrences", ascending=False).to_dict("records"),
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "backgroundColor": "#222", "color": "white"},
        style_header={"backgroundColor": "#333", "fontWeight": "bold"}
    )
], fluid=True)

app.layout = dbc.Container([
    html.H2("📊 Dashboard de Métricas de Emails", className="text-center text-warning my-3"),
    dbc.Tabs([
        dbc.Tab(label="🎯 Filtros & Resumen", tab_id="filtros", children=filter_layout),
        dbc.Tab(label="📈 Histórico Total", tab_id="historico", children=historico_layout),
    ])
], fluid=True)

# ================== Callback ==================
@app.callback(
    Output("kpi-total", "children"),
    Output("kpi-with-email", "children"),
    Output("kpi-valid", "children"),
    Output("kpi-sendable", "children"),
    Output("kpi-unique-sendable", "children"),
    Output("filtered-perc-bar", "figure"),
    Output("filtered-dq-pie", "figure"),
    Output("filtered-top-domains", "figure"),
    Output("filtered-duplicated-emails", "figure"),
    Input("apply-filters", "n_clicks"),
    State("agency-filter", "value"),
    State("destination-filter", "value"),
    State("cond-filter", "value"),
    State("localizador-filter", "value"),
    State("date-range", "start_date"),
    State("date-range", "end_date"),
)
def update_filtered(n, agency, dest, cond, localizador, start_date, end_date):
    if not n:
        raise dash.exceptions.PreventUpdate

    df_full = load_df_full(start_date, end_date)

    if agency:
        df_full = df_full[df_full["agency"].astype(str).str.contains(agency, case=False, na=False)]
    if dest:
        df_full = df_full[df_full["Destination"].astype(str).str.contains(dest, case=False, na=False)]
    if cond:
        df_full = df_full[df_full["condactivacion"].astype(str).str.contains(cond, case=False, na=False)]
    if localizador:
        df_full = df_full[df_full["Localizador"].astype(str).str.contains(localizador, case=False, na=False)]

    email_series = df_full[EMAIL_COLUMN].astype(str).str.strip()
    has_email = email_series.ne("") & email_series.notna()
    valid_format = has_email & email_series.str.match(EMAIL_REGEX)

    dup_counts = email_series[valid_format].str.lower().value_counts()
    dup_mask = email_series.str.lower().map(dup_counts).fillna(0) > 1

    total = int(len(df_full))
    with_email = int(has_email.sum())
    valid = int(valid_format.sum())
    sendable = int(valid)
    unique_sendable = int(email_series[valid_format].str.lower().nunique())

    empty_cnt   = int((~has_email).sum())
    invalid_cnt = int((has_email & ~valid_format).sum())
    duplicate_cnt = int(dup_mask.sum() - (dup_counts > 1).shape[0])

    k1 = kpi_card(total, "Total registros", "blue")
    k2 = kpi_card(with_email, "Con email", "blue")
    k3 = kpi_card(valid, "Válidos", "blue")
    k4 = kpi_card(sendable, "Enviables", "blue")
    k5 = kpi_card(unique_sendable, "Únicos válidos", "blue")

    fig_perc = bar_percentages(total, with_email, valid, unique_sendable, sendable)
    fig_dq   = dq_pie(valid, duplicate_cnt, empty_cnt, invalid_cnt)

    dom = email_series[valid_format].str.split("@").str[-1]
    dom_counts = dom.value_counts().head(10).reset_index()
    dom_counts.columns = ["domain", "count"]
    fig_domains = px.bar(dom_counts, x="domain", y="count", title="Top dominios (enviables del rango)")

    dup_top = dup_counts[dup_counts > 1].head(20).reset_index()
    dup_top.columns = ["email", "occurrences"]
    fig_dup = px.bar(dup_top, x="occurrences", y="email", orientation="h", title="Correos duplicados (rango)")

    return k1, k2, k3, k4, k5, fig_perc, fig_dq, fig_domains, fig_dup


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=7860, debug=True)
