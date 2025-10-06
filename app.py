#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard de Métricas de Emails — Opción Final 🧠✨
- Pestaña 1: filtros sobre data_full.parquet (análisis puntual)
- Pestaña 2: histórico simple desde agregados (ligero y veloz)
"""

import os
import re
from datetime import date
import pandas as pd

import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

# ================== Config ==================
DATA_DIR     = os.getenv("DATA_DIR", "data")
DATE_COLUMN  = os.getenv("DATE_COLUMN", "Fecha_de_creacion")
EMAIL_COLUMN = os.getenv("EMAIL_COLUMN", "Email")

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# ================== Carga de archivos ==================
print("📥 Cargando data_full.parquet...")
df_full = pd.read_parquet(os.path.join(DATA_DIR, "data_full.parquet"))
df_full[DATE_COLUMN] = pd.to_datetime(df_full[DATE_COLUMN], errors="coerce").dt.date

metrics_daily = pd.read_parquet(os.path.join(DATA_DIR, "metrics_daily.parquet"))
metrics_daily["metric_date"] = pd.to_datetime(metrics_daily["metric_date"]).dt.date

top_domains_daily = pd.read_parquet(os.path.join(DATA_DIR, "metrics_top_domains_daily.parquet"))
repeated_hist = pd.read_parquet(os.path.join(DATA_DIR, "metrics_repeated_emails.parquet"))

# ================== Opciones de filtros ==================
def _opts(series):
    vals = sorted([str(v) for v in series.dropna().unique()])
    return [{"label": v, "value": v} for v in vals]

agency_options      = _opts(df_full.get("agency", pd.Series(dtype=object)))
destination_options = _opts(df_full.get("Destination", pd.Series(dtype=object)))
cond_options        = _opts(df_full.get("condactivacion", pd.Series(dtype=object)))

date_min = df_full[DATE_COLUMN].min()
date_max = df_full[DATE_COLUMN].max()

# ================== Helpers UI ==================
def kpi_card(value, title, color="dark"):
    """
    Tarjeta KPI con título en negro para mejor contraste.
    """
    bg_colors = {
        "dark": "bg-dark",
        "blue": "bg-primary",
        "green": "bg-success",
        "orange": "bg-warning",
        "red": "bg-danger",
    }

    title_class = "text-dark fw-semibold d-block mb-1 text-center"
    value_class = "text-light fw-bold mb-0 text-center"

    # Para fondos claros como orange, ponemos el número en negro
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
    vals  = [ok,  duplicate,  empty,  invalid]
    fig = px.pie(names=names, values=vals, title="Razones DQ (del rango)")
    return fig

# ================== App ==================
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
server = app.server  # <-- añade esta línea
app.title = "📊 Dashboard de Emails"

# ================== Layout: Filtros ==================
filter_layout = dbc.Container([
    dbc.Row([
        dbc.Col([html.Label("Agency"), dcc.Dropdown(agency_options, id="agency-filter", multi=True)], md=3),
        dbc.Col([html.Label("Destination"), dcc.Dropdown(destination_options, id="destination-filter", multi=True)], md=3),
        dbc.Col([html.Label("Condactivacion"), dcc.Dropdown(cond_options, id="cond-filter", multi=True)], md=3),
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

    # KPIs (con títulos en negro)
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

# ================== Layout: Histórico ==================
hist_total_rows     = int(metrics_daily["total_rows"].sum())
hist_with_email     = int(metrics_daily["with_email"].sum())
hist_valid          = int(metrics_daily["valid_emails"].sum())
hist_sendable       = int(metrics_daily["sendable_emails"].sum()) if "sendable_emails" in metrics_daily else hist_valid
hist_unique_valid   = int(metrics_daily["unique_valid_emails"].sum()) if "unique_valid_emails" in metrics_daily else None
hist_first = metrics_daily["metric_date"].min()
hist_last  = metrics_daily["metric_date"].max()
hist_days  = metrics_daily["metric_date"].nunique()

historico_layout = dbc.Container([
    html.Hr(),
    dbc.Row([
        dbc.Col(kpi_card(hist_total_rows, "Registros (histórico)", "blue"), md=2),
        dbc.Col(kpi_card(hist_with_email, "Con email (suma)", "green"), md=2),
        dbc.Col(kpi_card(hist_valid, "Formato válido (suma)", "orange"), md=3),
        dbc.Col(kpi_card(hist_sendable, "Enviables (suma)", "green"), md=3),
        dbc.Col(kpi_card(hist_unique_valid or 0, "Únicos válidos (suma)", "dark"), md=2),
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

# ================== App layout ==================
app.layout = dbc.Container([
    html.H2("📊 Dashboard de Métricas de Emails", className="text-center text-warning my-3"),
    dbc.Tabs([
        dbc.Tab(label="🎯 Filtros & Resumen", tab_id="filtros", children=filter_layout),
        dbc.Tab(label="📈 Histórico Total", tab_id="historico", children=historico_layout),
    ])
], fluid=True)

# ================== Callbacks ==================
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

    dff = df_full[
        (df_full[DATE_COLUMN] >= pd.to_datetime(start_date).date()) &
        (df_full[DATE_COLUMN] <= pd.to_datetime(end_date).date())
    ].copy()

    if agency:
        dff = dff[dff["agency"].isin(agency)]
    if dest:
        dff = dff[dff["Destination"].isin(dest)]
    if cond:
        dff = dff[dff["condactivacion"].isin(cond)]
    if localizador:
        dff = dff[dff["Localizador"].astype(str).str.contains(str(localizador), na=False, case=False)]

    email_series = dff[EMAIL_COLUMN].astype(str).str.strip()
    has_email = email_series.ne("") & email_series.notna()
    valid_format = has_email & email_series.str.match(EMAIL_REGEX)

    dup_counts = email_series[valid_format].str.lower().value_counts()
    dup_mask = email_series.str.lower().map(dup_counts).fillna(0) > 1

    total = int(len(dff))
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

# ================== MAIN ==================
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8050, debug=True)
