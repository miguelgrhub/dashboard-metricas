# Email Metrics Dashboard (ETL + Dash)

Este proyecto genera métricas diarias a partir de una tabla grande de MySQL y expone un dashboard ligero
que puede compartirse dentro o fuera de la oficina sin consultar los 5M de registros en cada vista.

## Estructura

```
email_dashboard/
├─ update_metrics.py         # ETL: lee MySQL por chunks, calcula métricas y guarda agregados (MySQL + Parquet)
├─ app.py                    # Dashboard Dash (lee Parquet por defecto)
├─ schema.sql                # Tablas sugeridas para métricas agregadas
├─ .env.example              # Variables de entorno (duplica como .env y edita valores)
├─ requirements.txt
└─ data/                     # Parquet/CSV agregados
```

## Requisitos

- Python 3.10+
- MySQL con tabla `ContactsDetail.data` (o la tuya)
- Instalar dependencias:
```
pip install -r requirements.txt
```

## Configuración

1. Copia `.env.example` a `.env` y actualiza las variables:
   - `DATABASE_URL` con tu cadena de conexión (usa variables de entorno, no hardcodees credenciales).
   - `TABLE_NAME`, `EMAIL_COLUMN`, `DATE_COLUMN` (ejemplo: `Referido en fecha`), etc.

2. (Opcional) Crea tablas de agregados en MySQL:
```
mysql -h <host> -u <user> -p <db> < schema.sql
```

## Ejecutar ETL

Procesamiento total:
```
python update_metrics.py --full-rebuild
```

Procesamiento incremental por fechas (YYYY-MM-DD):
```
python update_metrics.py --start 2025-09-01 --end 2025-09-30
```

Esto genera:
- Tablas agregadas en MySQL (`metrics_daily`, `metrics_top_domains_daily`, `metrics_repeated_emails`).
- Archivos Parquet/CSV en `data/` (para dashboard sin DB).

## Ejecutar Dashboard

Por defecto lee Parquet locales. Lanza el servidor en LAN:
```
python app.py
```
Abre: http://127.0.0.1:8050 (o http://<IP_de_tu_PC>:8050 en tu red).

## Despliegue externo (ligero)

- Subir solo `app.py` + carpeta `data/` a Render/Railway/Streamlit Cloud.
- O publicarlo vía túnel seguro (Cloudflare Tunnel / ngrok).
- No expongas la base original: el dashboard usa **agregados** (rápidos y livianos).

## Notas

- La detección de emails válidos usa regex conservadora. Puedes ajustar reglas (p.ej. excluir roles: `info@`, `noreply@`).
- Si tu columna de fecha tiene espacios (ej. `Referido en fecha`), **déjala igual** y configúrala tal cual en `.env`.
- `metrics_repeated_emails` se calcula global (acumulando por chunks) para tener primeras/últimas fechas reales.
