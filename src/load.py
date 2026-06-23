"""
load.py — Fase 7: Carga Optimizada en DuckDB sin toPandas()
Proyecto: ETL Spark Parquet Advanced – NYC TLC Trip Records

ESTRATEGIA DE CARGA SIN OOM:
  DuckDB lee los archivos Parquet directamente desde disco con read_parquet().
  Esto elimina la cadena Spark→Python→Pandas→DuckDB que causa OutOfMemoryError
  con 40M+ registros. El consumo de RAM extra en Python es practicamente cero.

  Flujo optimizado:
    Spark escribe Parquet en gold/  →  DuckDB lee Parquet nativo  →  tablas SQL
    (ya implementado en fases 5-6)       (esta fase)
"""

import os
import json
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame

from src.utils import get_current_timestamp


# ══════════════════════════════════════════════════════════════════
# 1. DDL — DEFINICIÓN DE TABLAS
# ══════════════════════════════════════════════════════════════════

DDL_STATEMENTS = {

"gold_trips_clean": """
CREATE TABLE IF NOT EXISTS gold_trips_clean (
    trip_id                VARCHAR,
    service_type           VARCHAR,
    pickup_datetime        TIMESTAMP,
    dropoff_datetime       TIMESTAMP,
    trip_duration_minutes  DOUBLE,
    trip_distance          DOUBLE,
    pickup_location_id     BIGINT,
    dropoff_location_id    BIGINT,
    payment_type           BIGINT,
    fare_amount            DOUBLE,
    tip_amount             DOUBLE,
    total_amount           DOUBLE,
    tip_percentage         DOUBLE,
    average_speed_mph      DOUBLE,
    fare_per_mile          DOUBLE,
    is_airport_trip        BOOLEAN,
    is_suspicious_trip     BOOLEAN,
    year                   INTEGER,
    month                  INTEGER,
    source_file            VARCHAR,
    processing_date        DATE,
    quality_status         VARCHAR
);
""",

"gold_daily_revenue": """
CREATE TABLE IF NOT EXISTS gold_daily_revenue (
    service_type           VARCHAR,
    trip_date              DATE,
    total_trips            BIGINT,
    total_revenue          DOUBLE,
    average_fare           DOUBLE,
    average_tip            DOUBLE,
    average_trip_distance  DOUBLE,
    average_trip_duration  DOUBLE,
    suspicious_trips       BIGINT,
    quality_percentage     DOUBLE
);
""",

"gold_location_performance": """
CREATE TABLE IF NOT EXISTS gold_location_performance (
    service_type           VARCHAR,
    pickup_location_id     BIGINT,
    dropoff_location_id    BIGINT,
    total_trips            BIGINT,
    total_revenue          DOUBLE,
    average_fare           DOUBLE,
    average_distance       DOUBLE,
    average_duration       DOUBLE,
    suspicious_trip_count  BIGINT,
    average_speed_mph      DOUBLE
);
""",

"quality_rejected_records": """
CREATE TABLE IF NOT EXISTS quality_rejected_records (
    process_id          VARCHAR,
    trip_id             VARCHAR,
    service_type        VARCHAR,
    source_file         VARCHAR,
    rejection_stage     VARCHAR,
    rejection_rule_id   VARCHAR,
    rejection_rule      VARCHAR,
    rejection_column    VARCHAR,
    original_value      VARCHAR,
    technical_reason    VARCHAR,
    business_reason     VARCHAR,
    rejection_category  VARCHAR,
    rejected_at         TIMESTAMP
);
""",

"quality_metrics_summary": """
CREATE TABLE IF NOT EXISTS quality_metrics_summary (
    process_id             VARCHAR,
    service_type           VARCHAR,
    year                   INTEGER,
    month                  INTEGER,
    total_records          BIGINT,
    valid_records          BIGINT,
    rejected_records       BIGINT,
    duplicate_records      BIGINT,
    null_critical_records  BIGINT,
    suspicious_records     BIGINT,
    quality_percentage     DOUBLE,
    processed_at           TIMESTAMP
);
""",

"audit_file_inventory": """
CREATE TABLE IF NOT EXISTS audit_file_inventory (
    process_id      VARCHAR,
    source_system   VARCHAR,
    service_type    VARCHAR,
    file_name       VARCHAR,
    file_path       VARCHAR,
    file_size_mb    DOUBLE,
    partition_year  VARCHAR,
    partition_month VARCHAR,
    read_status     VARCHAR,
    record_count    BIGINT,
    column_count    INTEGER,
    schema_hash     VARCHAR,
    error_message   VARCHAR,
    processed_at    TIMESTAMP
);
""",
}


# ══════════════════════════════════════════════════════════════════
# 2. CONEXIÓN Y CONFIGURACIÓN DE DUCKDB
# ══════════════════════════════════════════════════════════════════

def get_duckdb_connection(config: Dict[str, Any]) -> duckdb.DuckDBPyConnection:
    """
    Abre DuckDB configurado para máximo rendimiento analítico.
    DuckDB gestiona su propia memoria interna; no compite con Spark.
    """
    db_path = str(Path(config["database"]["path"]).resolve())
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path)
    # Usar hasta 8 threads y 4GB para DuckDB (independiente de Spark)
    con.execute("SET threads = 4;")
    con.execute("SET memory_limit = '4GB';")
    # Habilitar lectura de Parquet particionado estilo Hive
    con.execute("SET enable_progress_bar = false;")
    return con


def initialize_schema(con: duckdb.DuckDBPyConnection, logger) -> None:
    """Crea todas las tablas si no existen (idempotente)."""
    logger.info("  Inicializando esquema DuckDB...")
    for table_name, ddl in DDL_STATEMENTS.items():
        try:
            con.execute(ddl)
            logger.info(f"    [OK] Tabla lista: {table_name}")
        except Exception as e:
            logger.error(f"    [ERR] Error creando {table_name}: {e}")
            raise


# ══════════════════════════════════════════════════════════════════
# 3. CARGA DIRECTA PARQUET → DUCKDB  (CERO toPandas)
# ══════════════════════════════════════════════════════════════════

def _parquet_pattern(parquet_dir: str) -> str:
    """
    Genera el patrón glob para que DuckDB encuentre todos los Parquet
    incluyendo subdirectorios de partición (service_type=yellow/year=2023/...).
    Convierte rutas Windows a formato compatible con DuckDB.
    """
    abs_dir = str(Path(parquet_dir).resolve())
    # DuckDB necesita forward slashes incluso en Windows
    if os.name == "nt":
        abs_dir = abs_dir.replace("\\", "/")
    return f"{abs_dir}/**/*.parquet"


def load_parquet_to_duckdb(
    con:          duckdb.DuckDBPyConnection,
    parquet_dir:  str,
    table_name:   str,
    process_id:   str,
    logger,
    batch_size_mb: int = 512,
    extra_cols:   Dict[str, str] = None,
) -> int:
    """
    Carga una tabla Gold leyendo Parquet directamente con DuckDB.

    VENTAJA CLAVE vs toPandas():
      - Sin paso Spark→Python→Pandas: el dato pasa disco→DuckDB
      - DuckDB usa su propio motor columnar, no la RAM de Spark
      - Soporta 100M+ registros con ~500MB de RAM
      - Lee archivos particionados en paralelo automáticamente

    Args:
        parquet_dir:   Directorio con archivos Parquet (particionados o no)
        table_name:    Tabla destino en DuckDB
        extra_cols:    Columnas literales a agregar (ej: {'process_id': 'ETL-123'})
        batch_size_mb: Control de memoria interna de DuckDB (no afecta Spark)
    """
    pattern = _parquet_pattern(parquet_dir)
    logger.info(f"    [READ] {table_name} ← {pattern}")

    # Verificar que existen archivos
    parquet_files = list(Path(parquet_dir).rglob("*.parquet"))
    if not parquet_files:
        logger.warning(f"    [SKIP] Sin archivos Parquet en: {parquet_dir}")
        return 0

    logger.info(f"    [FILES] {len(parquet_files)} archivo(s) Parquet encontrados")

    # Limpiar datos anteriores del mismo process_id (idempotencia)
    table_info = con.execute(f"DESCRIBE {table_name}").fetchall()
    table_cols  = [r[0] for r in table_info]
    if "process_id" in table_cols:
        con.execute(f"DELETE FROM {table_name} WHERE process_id = ?", [process_id])
    else:
        con.execute(f"DELETE FROM {table_name} WHERE 1=1")

    # ── Construir SELECT con columnas extra opcionales ─────────────
    if extra_cols:
        extras = ", ".join(f"'{v}' AS {k}" for k, v in extra_cols.items())
        select_clause = f"*, {extras}"
    else:
        select_clause = "*"

    try:
        # Lectura nativa DuckDB — sin pasar por Python ni Pandas
        insert_sql = f"""
            INSERT INTO {table_name} BY NAME
            SELECT {select_clause}
            FROM read_parquet('{pattern}', hive_partitioning = true,
                              union_by_name = true)
        """
        con.execute(insert_sql)
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"    [OK]   {table_name}: {count:,} registros totales")
        return count

    except Exception as e:
        # Fallback: leer sin hive_partitioning si hay columnas virtuales extra
        logger.warning(f"    [WARN] Hive partitioning falló ({e}), reintentando simple...")
        try:
            insert_sql_simple = f"""
                INSERT INTO {table_name} BY NAME
                SELECT {select_clause}
                FROM read_parquet('{pattern}', union_by_name = true)
            """
            con.execute(insert_sql_simple)
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"    [OK]   {table_name}: {count:,} registros (modo simple)")
            return count
        except Exception as e2:
            logger.error(f"    [ERR]  {table_name}: {e2}")
            raise


def load_inventory_table(
    con:               duckdb.DuckDBPyConnection,
    inventory_records: List[Dict[str, Any]],
    process_id:        str,
    logger,
) -> int:
    """
    Carga el inventario de archivos (lista pequeña de dicts).
    Esta es la ÚNICA tabla que usa Pandas, justificado porque son
    máximo ~20 registros (uno por archivo), no millones.
    """
    if not inventory_records:
        logger.warning("    [SKIP] Inventario vacío")
        return 0

    df = pd.DataFrame(inventory_records)

    # Coerción de tipos
    if "processed_at" in df.columns:
        df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")
    for col in ["record_count", "column_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
    if "file_size_mb" in df.columns:
        df["file_size_mb"] = pd.to_numeric(df["file_size_mb"], errors="coerce").fillna(0.0)

    try:
        con.execute("DELETE FROM audit_file_inventory WHERE process_id = ?", [process_id])
        con.execute("INSERT INTO audit_file_inventory SELECT * FROM df")
        logger.info(f"    [OK]   audit_file_inventory: {len(df)} archivos")
        return len(df)
    except Exception as e:
        logger.error(f"    [ERR]  audit_file_inventory: {e}")
        raise


# ══════════════════════════════════════════════════════════════════
# 4. CONSULTAS DE VERIFICACIÓN
# ══════════════════════════════════════════════════════════════════

VERIFICATION_QUERIES = [
    {
        "name": "V1 — Viajes por servicio e ingresos",
        "sql": """
SELECT service_type,
       COUNT(*)                         AS total_trips,
       ROUND(SUM(total_amount), 2)      AS total_revenue,
       ROUND(AVG(fare_amount), 2)       AS avg_fare,
       ROUND(AVG(trip_duration_minutes),2) AS avg_duration_min,
       ROUND(AVG(trip_distance), 2)     AS avg_distance_miles
FROM gold_trips_clean
GROUP BY service_type
ORDER BY total_revenue DESC;""",
    },
    {
        "name": "V2 — Métricas de calidad por servicio/año/mes",
        "sql": """
SELECT service_type, year, month,
       total_records, valid_records, rejected_records,
       suspicious_records,
       ROUND(quality_percentage, 2) AS quality_pct
FROM quality_metrics_summary
ORDER BY year, month, service_type;""",
    },
    {
        "name": "V3 — Top 20 rutas por ingresos",
        "sql": """
SELECT pickup_location_id, dropoff_location_id,
       COUNT(*)                         AS total_trips,
       ROUND(SUM(total_amount), 2)      AS total_revenue,
       ROUND(AVG(trip_duration_minutes),2) AS avg_duration
FROM gold_trips_clean
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY total_revenue DESC
LIMIT 20;""",
    },
    {
        "name": "V4 — Inventario de archivos por estado",
        "sql": """
SELECT service_type, read_status,
       COUNT(*) AS files,
       SUM(record_count) AS records,
       ROUND(SUM(file_size_mb),2) AS size_mb
FROM audit_file_inventory
GROUP BY service_type, read_status
ORDER BY service_type;""",
    },
    {
        "name": "V5 — Motivos de rechazo más frecuentes",
        "sql": """
SELECT rejection_category, rejection_rule, service_type,
       COUNT(*) AS rejected_records
FROM quality_rejected_records
GROUP BY rejection_category, rejection_rule, service_type
ORDER BY rejected_records DESC
LIMIT 20;""",
    },
    {
        "name": "V6 — Top 10 días por ingresos",
        "sql": """
SELECT trip_date, service_type, total_trips,
       ROUND(total_revenue, 2)      AS total_revenue,
       ROUND(average_fare, 2)       AS avg_fare,
       ROUND(quality_percentage, 2) AS quality_pct
FROM gold_daily_revenue
ORDER BY total_revenue DESC
LIMIT 10;""",
    },
    {
        "name": "V7 — Viajes sospechosos por servicio",
        "sql": """
SELECT service_type,
       COUNT(*)  AS total_trips,
       SUM(CASE WHEN is_suspicious_trip THEN 1 ELSE 0 END) AS suspicious,
       ROUND(SUM(CASE WHEN is_suspicious_trip THEN 1 ELSE 0 END)::DOUBLE
             / COUNT(*)::DOUBLE * 100, 2) AS suspicious_pct
FROM gold_trips_clean
GROUP BY service_type
ORDER BY suspicious_pct DESC;""",
    },
    {
        "name": "V8 — Integridad de fechas por partición",
        "sql": """
SELECT service_type, year, month,
       COUNT(*) AS trips,
       MIN(pickup_datetime)::DATE AS earliest,
       MAX(pickup_datetime)::DATE AS latest
FROM gold_trips_clean
GROUP BY service_type, year, month
ORDER BY service_type, year, month;""",
    },
]


def run_verification_queries(
    con: duckdb.DuckDBPyConnection,
    logger,
) -> List[Dict[str, Any]]:
    """Ejecuta las 8 consultas de verificación SQL obligatorias."""
    logger.info("\n[7.3] CONSULTAS DE VERIFICACIÓN SQL:")
    results = []
    for qd in VERIFICATION_QUERIES:
        logger.info(f"\n  ── {qd['name']} ──")
        try:
            df_result = con.execute(qd["sql"]).df()
            logger.info(f"\n{df_result.to_string(index=False)}\n")
            results.append({
                "name":   qd["name"],
                "rows":   len(df_result),
                "status": "OK",
                "data":   df_result.to_dict(orient="records"),
            })
        except Exception as e:
            logger.error(f"  ERROR: {e}")
            results.append({"name": qd["name"], "rows": 0, "status": f"ERROR:{e}"})
    return results


# ══════════════════════════════════════════════════════════════════
# 5. FASE 7 COMPLETA — CARGA OPTIMIZADA SIN toPandas
# ══════════════════════════════════════════════════════════════════

# Mapeo: nombre_tabla → subdirectorio en gold/ donde están los Parquet
GOLD_PARQUET_DIRS = {
    "gold_trips_clean":          "trips",
    "gold_daily_revenue":        "daily",
    "gold_location_performance": "location",
}
AUDIT_PARQUET_DIRS = {
    "quality_rejected_records": "records",     # en quarantine/
    "quality_metrics_summary":  "quality_metrics",  # en audit/
}


def run_load_phase(
    spark:             SparkSession,
    tables:            Dict[str, DataFrame],   # Ya no se usa para la carga
    inventory_records: List[Dict[str, Any]],
    config:            Dict[str, Any],
    process_id:        str,
    logger,
) -> List[Dict[str, Any]]:
    """
    Fase 7 optimizada: carga directa Parquet → DuckDB sin toPandas().

    Los DataFrames Spark (parámetro tables) NO se convierten a Pandas.
    En su lugar, DuckDB lee los archivos Parquet escritos en las fases
    anteriores directamente desde disco, con su propio motor columnar.

    Esto resuelve el OOM con 40M+ registros.
    """
    logger.info("═" * 70)
    logger.info(f"  FASE 7: CARGA EN DUCKDB (Parquet nativo)  —  {process_id}")
    logger.info(f"  Motor: DuckDB — {config['database']['path']}")
    logger.info("═" * 70)

    # Rutas base (absolutas)
    gold_dir  = str(Path(config["paths"]["gold_dir"]).resolve())
    quar_dir  = str(Path(config["paths"]["quarantine_dir"]).resolve())
    audit_dir = str(Path(config["paths"]["audit_dir"]).resolve())

    con = get_duckdb_connection(config)

    try:
        # ── Inicializar esquema ────────────────────────────────────
        logger.info("\n[7.1] Inicializando esquema DuckDB...")
        initialize_schema(con, logger)

        total_loaded = 0
        logger.info("\n[7.2] Cargando tablas (Parquet → DuckDB sin Pandas):")

        # ── Tablas Gold ────────────────────────────────────────────
        for table_name, subdir in GOLD_PARQUET_DIRS.items():
            parquet_path = os.path.join(gold_dir, subdir)
            n = load_parquet_to_duckdb(
                con, parquet_path, table_name, process_id, logger
            )
            total_loaded += n

        # ── Tabla de rechazados (en quarantine/) ───────────────────
        rejected_path = os.path.join(quar_dir, "records")
        n = load_parquet_to_duckdb(
            con, rejected_path, "quality_rejected_records", process_id, logger
        )
        total_loaded += n

        # ── Tabla de métricas (en audit/) ──────────────────────────
        metrics_path = os.path.join(audit_dir, "quality_metrics")
        n = load_parquet_to_duckdb(
            con, metrics_path, "quality_metrics_summary", process_id, logger
        )
        total_loaded += n

        # ── Inventario de archivos (lista pequeña, usa Pandas OK) ──
        n = load_inventory_table(con, inventory_records, process_id, logger)
        total_loaded += n

        logger.info(f"\n  Total registros en DuckDB: {total_loaded:,}")

        # ── Verificación SQL ───────────────────────────────────────
        verification_results = run_verification_queries(con, logger)

        # Guardar reporte
        report = {"process_id": process_id, "queries": verification_results}
        report_path = Path(audit_dir) / f"verification_{process_id}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"\n  Reporte: {report_path}")
        logger.info("═" * 70)
        return verification_results

    finally:
        con.close()


def query_duckdb(config: Dict[str, Any], sql: str) -> pd.DataFrame:
    """Ejecuta SQL en DuckDB y retorna DataFrame Pandas (solo para análisis pequeños)."""
    con = get_duckdb_connection(config)
    try:
        return con.execute(sql).df()
    finally:
        con.close()
