#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
📊 Dashboard de Métricas de Emails — Versión Streamlit
"""

import os
import re
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime

# ================== Config ==================
DATA_DIR     = os.getenv("DATA_DIR", "data")
DATE_COLUMN  = os.getenv("DATE_COLUMN", "Fecha_de_creacion")
EMAIL_COLUMN = os.getenv("EMAIL_COLUMN", "Email")
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# ================== Cache de datos ==================
@st.cache_data
def load_data():
    df_full = pd.read_parquet(os.path.join(DATA_DIR, "data_full.parquet"))
    df_full[DATE_COLUMN] = pd.to_datetime(df_full[DATE_COLUMN], errors="coerce").dt.date

    metrics_daily = pd.read_parquet(os.path.join(DATA_DIR, "metrics_daily.parquet"))
    metrics_daily["metric_date"] = pd.to_datetime(metrics_daily["metric_date"]).dt.date

    top_domains_daily = pd.read_parquet(os.path.join(DATA_DIR, "metrics_top_domains_daily.parquet"))
    repeated_hist = pd.read_parquet(os.path.join(DATA_DIR, "metrics_repeated_emails.parquet"))

    return df_full, metrics_daily, top_domains_daily, repeated_hist

df_full, metrics_daily, top_domains_daily, repeated_hist = load_data()

# ================== Sidebar filtros ==================
st.sidebar.title("🎯 Filtros")

agency_options = sorted(df_full["agency"].dropna().unique())
destination_options = sorted(df_full["Destination"].dropna().unique())
cond_options = sorted(df_full["condactivacion"].dropna().unique())

agency_filter = st.sidebar.multiselect("Agency", agency_options)
dest_filter = st.sidebar.multiselect("Destination", destination_options)
cond_filter = st.sidebar.multiselect("Condactivacion", cond_options)
localizador_filter = st.sidebar.text_input("Localizador")

date_min = df_full[DATE_COLUMN].min()
date_max = df_full[DATE_COLUMN].max()
date_range = st.sidebar.date_input(
    "Rango de fechas", [date_min, date_max],
    min_value=date_min, max_value=date_max
)

st.sidebar.markdown("---")

# ================== Tabs ==================
tab1, tab2 = st.tabs(["📌 Filtros & Resumen", "📈 Histórico Total"])

with tab1:
    st.header("📌 Resumen del Rango Seleccionado")

    # Filtro de DataFrame
    dff = df_full.copy()
    dff = dff[(dff[DATE_COLUMN] >= date_range[0]) & (dff[DATE_COLUMN] <= date_range[1])]
    if agency_filter:
        dff = dff[dff["agency"].isin(agency_filter)]
    if dest_filter:
        dff = dff[dff["Destination"].isin(dest_filter)]
    if cond_filter:
        dff = dff[dff["condactivacion"].isin(cond_filter)]
    if localizador_filter:
        dff = dff[dff["Localizador"].astype(str).str.contains(localizador_filter, case=False, na=False)]

    email_series = dff[EMAIL_COLUMN].astype(str).str.strip()
    has_email = email_series.ne("") & email_series.notna()
    valid_format = has_email & email_series.str.match(EMAIL_REGEX)

    total = len(dff)
    with_email = int(has_email.sum())
    valid = int(valid_format.sum())
    sendable = valid
    unique_sendable = int(email_series[valid_format].str.lower().nunique())

    empty_cnt   = int((~has_email).sum())
    invalid_cnt = int((has_email & ~valid_format).sum())
    dup_counts = email_series[valid_format].str.lower().value_counts()
    duplicate_cnt = int((dup_counts > 1).sum())

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total", f"{total:,}")
    col2.metric("Con email", f"{with_email:,}")
    col3.metric("Válidos", f"{valid:,}")
    col4.metric("Enviables", f"{sendable:,}")
    col5.metric("Únicos válidos", f"{unique_sendable:,}")

    # Gráfico de porcentajes
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
        (unique_sendable / total * 100) if total else 0,
        (sendable / total * 100) if total else 0,
    ]
    fig_bar = px.bar(x=cats, y=vals, title="Volumen del día (porcentajes)")
    fig_bar.update_traces(text=[f"{v:.1f}%" for v in vals], textposition="outside")
    tab1.plotly_chart(fig_bar, use_container_width=True)

    # Pie chart DQ
    fig_pie = px.pie(
        names=["ok", "duplicate", "empty", "invalid_format"],
        values=[valid, duplicate_cnt, empty_cnt, invalid_cnt],
        title="Razones DQ (del rango)"
    )
    tab1.plotly_chart(fig_pie, use_container_width=True)

    # Top dominios
    dom_counts = email_series[valid_format].str.split("@").str[-1].value_counts().head(10).reset_index()
    dom_counts.columns = ["domain", "count"]
    fig_domains = px.bar(dom_counts, x="domain", y="count", title="Top dominios")
    tab1.plotly_chart(fig_domains, use_container_width=True)

    # Duplicados
    dup_top = dup_counts[dup_counts > 1].head(20).reset_index()
    dup_top.columns = ["email", "occurrences"]
    fig_dup = px.bar(dup_top, x="occurrences", y="email", orientation="h", title="Correos duplicados")
    tab1.plotly_chart(fig_dup, use_container_width=True)

with tab2:
    st.header("📈 Histórico Total")
    hist_total_rows     = int(metrics_daily["total_rows"].sum())
    hist_with_email     = int(metrics_daily["with_email"].sum())
    hist_valid          = int(metrics_daily["valid_emails"].sum())
    hist_sendable       = int(metrics_daily["sendable_emails"].sum()) if "sendable_emails" in metrics_daily else hist_valid
    hist_unique_valid   = int(metrics_daily["unique_valid_emails"].sum()) if "unique_valid_emails" in metrics_daily else None

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Registros (histórico)", f"{hist_total_rows:,}")
    col2.metric("Con email", f"{hist_with_email:,}")
    col3.metric("Válidos", f"{hist_valid:,}")
    col4.metric("Enviables", f"{hist_sendable:,}")
    col5.metric("Únicos válidos", f"{hist_unique_valid or 0:,}")

    # Línea temporal
    melted = metrics_daily.melt(id_vars=["metric_date"], var_name="variable", value_name="value")
    fig_line = px.line(melted, x="metric_date", y="value", color="variable", title="Evolución de métricas diarias")
    st.plotly_chart(fig_line, use_container_width=True)

    # Top dominios histórico
    top10 = top_domains_daily.groupby("domain", as_index=False)["cnt"].sum().sort_values("cnt", ascending=False).head(10)
    fig_top_domains = px.bar(top10, x="domain", y="cnt", title="Top dominios más frecuentes (histórico)")
    st.plotly_chart(fig_top_domains, use_container_width=True)

    # Emails repetidos históricos
    st.subheader("Emails repetidos (histórico total)")
    st.dataframe(repeated_hist.sort_values("occurrences", ascending=False).head(50))
