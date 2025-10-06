#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL: Lee tabla grande por chunks, calcula métricas diarias, guarda en MySQL y Parquet,
y genera repeated_emails_daily.parquet + data_full.parquet para el dashboard.
Uso:
    python update_metrics.py --full-rebuild
    python update_metrics.py --start 2025-09-01 --end 2025-09-30
"""

import os
import argparse
import re
import sys
from datetime import datetime
from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ================== Config ==================
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_NAME   = os.getenv("TABLE_NAME", "data")

EMAIL_COLUMN = os.getenv("EMAIL_COLUMN", "Email")
DATE_COLUMN  = os.getenv("DATE_COLUMN", "Fecha_de_creacion")

OPENS_COLUMN  = os.getenv("OPENS_COLUMN", "")
CLICKS_COLUMN = os.getenv("CLICKS_COLUMN", "")

DATA_DIR = os.getenv("DATA_DIR", "data")
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "150000"))  # Ajusta según RAM

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# ================== Helpers ==================
def norm_col(col: str) -> str:
    return f"`{col}`"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def valid_email(e: str) -> bool:
    return isinstance(e, str) and e.strip() and EMAIL_REGEX.match(e.strip())

def email_domain(e: str) -> str | None:
    try:
        return e.split("@", 1)[1].lower().strip()
    except Exception:
        return None

# ================== Aggregators ==================
daily = defaultdict(lambda: {
    "total_rows": 0,
    "with_email": 0,
    "valid_emails": 0,
    "invalid_emails": 0,
    "duplicates_extra_rows": 0,
    "unique_valid_emails": 0,
    "sendable_emails": 0,
    "total_opens": 0,
    "total_clicks": 0
})

domains_daily = defaultdict(lambda: defaultdict(int))
email_global_counts = defaultdict(int)
email_first_seen = {}
email_last_seen  = {}

def process_chunk(chunk: pd.DataFrame, idx: int):
    print(f"▶ Procesando chunk {idx:,} ({len(chunk):,} filas)...")
    if EMAIL_COLUMN not in chunk.columns or DATE_COLUMN not in chunk.columns:
        raise RuntimeError(f"Chunk sin columnas esperadas. Tiene: {chunk.columns}")

    chunk[EMAIL_COLUMN] = chunk[EMAIL_COLUMN].astype(str).str.strip()
    chunk["_metric_date"] = pd.to_datetime(chunk[DATE_COLUMN], errors="coerce").dt.date

    for d, sub in chunk.groupby("_metric_date", dropna=True):
        dkey = d
        if pd.isna(dkey):
            continue

        daily[dkey]["total_rows"] += len(sub)

        with_email = sub[sub[EMAIL_COLUMN].str.len() > 0]
        daily[dkey]["with_email"] += len(with_email)

        valid_mask = with_email[EMAIL_COLUMN].str.lower().str.match(EMAIL_REGEX)
        valid_df = with_email[valid_mask].copy()
        invalid_df = with_email[~valid_mask].copy()

        daily[dkey]["valid_emails"] += len(valid_df)
        daily[dkey]["invalid_emails"] += len(invalid_df)

        vc = valid_df[EMAIL_COLUMN].str.lower().value_counts()
        duplicates_extra = int((vc[vc > 1] - 1).sum()) if not vc.empty else 0
        unique_valid = int(vc.shape[0]) if not vc.empty else 0

        daily[dkey]["duplicates_extra_rows"] += duplicates_extra
        daily[dkey]["unique_valid_emails"] += unique_valid
        daily[dkey]["sendable_emails"] += len(valid_df)

        if OPENS_COLUMN and OPENS_COLUMN in sub.columns:
            daily[dkey]["total_opens"] += pd.to_numeric(sub[OPENS_COLUMN], errors="coerce").fillna(0).sum()
        if CLICKS_COLUMN and CLICKS_COLUMN in sub.columns:
            daily[dkey]["total_clicks"] += pd.to_numeric(sub[CLICKS_COLUMN], errors="coerce").fillna(0).sum()

        if not valid_df.empty:
            doms = valid_df[EMAIL_COLUMN].str.lower().map(email_domain).dropna()
            for dom, cnt in doms.value_counts().items():
                domains_daily[dkey][dom] += int(cnt)

    valid_all = chunk[chunk[EMAIL_COLUMN].str.lower().str.match(EMAIL_REGEX)].copy()
    for _, row in valid_all[[EMAIL_COLUMN, "_metric_date"]].dropna().iterrows():
        e = str(row[EMAIL_COLUMN]).lower().strip()
        dt = row["_metric_date"]
        email_global_counts[e] += 1
        if e not in email_first_seen or dt < email_first_seen[e]:
            email_first_seen[e] = dt
        if e not in email_last_seen or dt > email_last_seen[e]:
            email_last_seen[e] = dt

def flush_to_mysql(engine):
    print("💾 Guardando métricas en MySQL...")

    daily_df = (
        pd.DataFrame.from_dict(daily, orient="index")
        .reset_index()
        .rename(columns={"index": "metric_date"})
        .sort_values("metric_date")
    )
    daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"]).dt.date

    tmp_table = "tmp_metrics_daily"
    with engine.begin() as conn:
        print("  - Subiendo metrics_daily (tmp)...")
        daily_df.to_sql(tmp_table, conn, if_exists="replace", index=False)
        print("  - Haciendo upsert en metrics_daily...")
        conn.execute(text(f"""
            INSERT INTO metrics_daily
            (metric_date, total_rows, with_email, valid_emails, invalid_emails, duplicates_extra_rows,
             unique_valid_emails, sendable_emails, total_opens, total_clicks)
            SELECT metric_date, total_rows, with_email, valid_emails, invalid_emails, duplicates_extra_rows,
                   unique_valid_emails, sendable_emails, total_opens, total_clicks
            FROM {tmp_table}
            ON DUPLICATE KEY UPDATE
              total_rows=VALUES(total_rows),
              with_email=VALUES(with_email),
              valid_emails=VALUES(valid_emails),
              invalid_emails=VALUES(invalid_emails),
              duplicates_extra_rows=VALUES(duplicates_extra_rows),
              unique_valid_emails=VALUES(unique_valid_emails),
              sendable_emails=VALUES(sendable_emails),
              total_opens=VALUES(total_opens),
              total_clicks=VALUES(total_clicks);
        """))
        conn.execute(text(f"DROP TABLE {tmp_table}"))
    print("✅ metrics_daily guardado")

    rows = []
    for d, doms in domains_daily.items():
        for dom, cnt in doms.items():
            rows.append({"metric_date": d, "domain": dom, "cnt": int(cnt)})
    mtd_df = pd.DataFrame(rows)
    if not mtd_df.empty:
        tmp_table = "tmp_metrics_top_domains_daily"
        with engine.begin() as conn:
            print("  - Guardando top dominios...")
            mtd_df.to_sql(tmp_table, conn, if_exists="replace", index=False)
            conn.execute(text(f"""
                INSERT INTO metrics_top_domains_daily (metric_date, domain, cnt)
                SELECT metric_date, domain, cnt
                FROM {tmp_table}
                ON DUPLICATE KEY UPDATE
                  cnt=VALUES(cnt);
            """))
            conn.execute(text(f"DROP TABLE {tmp_table}"))
        print("✅ Top dominios guardado")

    rep_rows = []
    for e, c in email_global_counts.items():
        rep_rows.append({
            "email": e,
            "occurrences": int(c),
            "first_seen": email_first_seen.get(e),
            "last_seen": email_last_seen.get(e)
        })
    rep_df = pd.DataFrame(rep_rows)
    if not rep_df.empty:
        tmp_table = "tmp_metrics_repeated_emails"
        with engine.begin() as conn:
            print("  - Guardando emails repetidos...")
            rep_df.to_sql(tmp_table, conn, if_exists="replace", index=False)
            conn.execute(text(f"""
                INSERT INTO metrics_repeated_emails (email, occurrences, first_seen, last_seen)
                SELECT email, occurrences, first_seen, last_seen
                FROM {tmp_table}
                ON DUPLICATE KEY UPDATE
                  occurrences=VALUES(occurrences),
                  first_seen=LEAST(COALESCE(metrics_repeated_emails.first_seen, VALUES(first_seen)), VALUES(first_seen)),
                  last_seen=GREATEST(COALESCE(metrics_repeated_emails.last_seen, VALUES(last_seen)), VALUES(last_seen));
            """))
            conn.execute(text(f"DROP TABLE {tmp_table}"))
        print("✅ Emails repetidos guardado")

    ensure_dir(DATA_DIR)
    print("💽 Guardando copias locales en Parquet...")
    daily_df.to_parquet(os.path.join(DATA_DIR, "metrics_daily.parquet"), index=False)
    if not mtd_df.empty:
        mtd_df.to_parquet(os.path.join(DATA_DIR, "metrics_top_domains_daily.parquet"), index=False)
    if not rep_df.empty:
        rep_df.to_parquet(os.path.join(DATA_DIR, "metrics_repeated_emails.parquet"), index=False)
    print("✅ Parquet guardado")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    parser.add_argument("--full-rebuild", action="store_true")
    args = parser.parse_args()

    if not DATABASE_URL:
        print("Falta DATABASE_URL en .env")
        sys.exit(1)

    print("🚀 Iniciando ETL...")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
    print("✅ Conectado a la base de datos")

    where_clause = ""
    params = {}
    if args.full_rebuild:
        print("📆 Procesando histórico completo")
    elif args.start and args.end:
        where_clause = f"WHERE {norm_col(DATE_COLUMN)} BETWEEN :start AND :end"
        params = {"start": args.start, "end": args.end}
        print(f"📆 Procesando rango: {args.start} → {args.end}")
    elif args.start:
        where_clause = f"WHERE {norm_col(DATE_COLUMN)} >= :start"
        params = {"start": args.start}
        print(f"📆 Procesando desde {args.start}")
    elif args.end:
        where_clause = f"WHERE {norm_col(DATE_COLUMN)} <= :end"
        params = {"end": args.end}
        print(f"📆 Procesando hasta {args.end}")

    select_cols = [f"{norm_col(EMAIL_COLUMN)} AS email", f"{norm_col(DATE_COLUMN)} AS created_at"]
    if OPENS_COLUMN:
        select_cols.append(f"{norm_col(OPENS_COLUMN)} AS opens")
    if CLICKS_COLUMN:
        select_cols.append(f"{norm_col(CLICKS_COLUMN)} AS clicks")

    sql = f"SELECT {', '.join(select_cols)} FROM {norm_col(TABLE_NAME)} {where_clause}"

    with engine.connect() as conn:
        for i, chunk in enumerate(pd.read_sql(text(sql), conn, params=params, chunksize=CHUNKSIZE), start=1):
            chunk = chunk.rename(columns={"email": EMAIL_COLUMN, "created_at": DATE_COLUMN})
            process_chunk(chunk, i)

    flush_to_mysql(engine)

    # ================== NUEVO BLOQUE: Parquets detallados ==================
    print("📌 Generando metrics_repeated_emails_daily.parquet con filtros adicionales...")
    with engine.connect() as conn:
        df_full = pd.read_sql(text(f"""
            SELECT Email, agency, Destination, condactivacion, Localizador, {DATE_COLUMN}
            FROM {TABLE_NAME}
        """), conn)

    df_full[DATE_COLUMN] = pd.to_datetime(df_full[DATE_COLUMN], errors="coerce")
    df_full = df_full.dropna(subset=[DATE_COLUMN])

    daily_df = (
        df_full.groupby([
            df_full["Email"].str.lower(),
            "agency",
            "Destination",
            "condactivacion",
            "Localizador",
            df_full[DATE_COLUMN].dt.date
        ])
        .size()
        .reset_index(name="occurrences")
        .rename(columns={"Email": "email", DATE_COLUMN: "metric_date"})
    )

    daily_df.to_parquet(os.path.join(DATA_DIR, "metrics_repeated_emails_daily.parquet"), index=False)
    print(f"✅ metrics_repeated_emails_daily.parquet guardado ({len(daily_df):,} filas)")

    print("📦 Generando data_full.parquet (detalle completo para dashboard)...")
    df_full_export = df_full[[
        "Email", "agency", "Destination", "condactivacion", "Localizador", DATE_COLUMN
    ]].copy()
    df_full_export[DATE_COLUMN] = pd.to_datetime(df_full_export[DATE_COLUMN], errors="coerce").dt.date
    df_full_export = df_full_export.dropna(subset=[DATE_COLUMN])
    df_full_export.to_parquet(os.path.join(DATA_DIR, "data_full.parquet"), index=False)
    print(f"✅ data_full.parquet guardado ({len(df_full_export):,} filas)")

    print("🎉 ETL completado correctamente ✅")

if __name__ == "__main__":
    main()
