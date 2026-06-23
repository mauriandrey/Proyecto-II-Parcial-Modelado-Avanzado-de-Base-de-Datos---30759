"""
schema_recovery.py — Fase 2: Diagnóstico y Reconstrucción de Esquema Canónico
Proyecto: ETL Spark Parquet Advanced – NYC TLC Trip Records

Responsabilidades:
  - Comparar esquema real de cada archivo contra el esquema esperado (JSON)
  - Detectar columnas faltantes, adicionales e incompatibles
  - Homologar columnas entre yellow, green y fhvhv al esquema canónico
  - Reconstruir DataFrames con el esquema unificado
  - Enviar archivos no recuperables a cuarentena con evidencia técnica
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType,
    IntegerType, BooleanType
)

from src.utils import compute_schema_hash, get_current_timestamp, load_metadata


# ══════════════════════════════════════════════════════════════════
# 1. ESQUEMA CANÓNICO PySpark
# ══════════════════════════════════════════════════════════════════

# Columnas del esquema canónico con sus tipos PySpark
# Todas las columnas extra de servicios específicos se incluyen con nullable=True
CANONICAL_SCHEMA_FIELDS = {
    "trip_id":               StringType(),
    "service_type":          StringType(),
    "vendor_id":             StringType(),
    "pickup_datetime":       TimestampType(),
    "dropoff_datetime":      TimestampType(),
    "passenger_count":       DoubleType(),
    "trip_distance":         DoubleType(),
    "pickup_location_id":    LongType(),
    "dropoff_location_id":   LongType(),
    "rate_code_id":          DoubleType(),
    "store_and_fwd_flag":    StringType(),
    "payment_type":          LongType(),
    "fare_amount":           DoubleType(),
    "extra_amount":          DoubleType(),
    "mta_tax":               DoubleType(),
    "tip_amount":            DoubleType(),
    "tolls_amount":          DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount":          DoubleType(),
    "congestion_surcharge":  DoubleType(),
    "airport_fee":           DoubleType(),
    "ehail_fee":             DoubleType(),   # solo green
    "trip_type":             DoubleType(),   # solo green
    "year":                  IntegerType(),
    "month":                 IntegerType(),
    "source_file":           StringType(),
    "ingestion_timestamp":   TimestampType(),
    "quality_status":        StringType(),
}

# Columnas críticas que NO pueden ser nulas en bronze
CRITICAL_COLUMNS = {
    "pickup_datetime", "dropoff_datetime",
    "pickup_location_id", "dropoff_location_id",
    "fare_amount", "total_amount",
}


# ══════════════════════════════════════════════════════════════════
# 2. MATRICES DE HOMOLOGACIÓN POR SERVICIO
# ══════════════════════════════════════════════════════════════════

# Mapeo: nombre_original_en_parquet → nombre_canónico
YELLOW_COLUMN_MAP = {
    "VendorID":              "vendor_id",
    "tpep_pickup_datetime":  "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count":       "passenger_count",
    "trip_distance":         "trip_distance",
    "RatecodeID":            "rate_code_id",
    "store_and_fwd_flag":    "store_and_fwd_flag",
    "PULocationID":          "pickup_location_id",
    "DOLocationID":          "dropoff_location_id",
    "payment_type":          "payment_type",
    "fare_amount":           "fare_amount",
    "extra":                 "extra_amount",
    "mta_tax":               "mta_tax",
    "tip_amount":            "tip_amount",
    "tolls_amount":          "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount":          "total_amount",
    "congestion_surcharge":  "congestion_surcharge",
    "Airport_fee":           "airport_fee",
}

GREEN_COLUMN_MAP = {
    "VendorID":              "vendor_id",
    "lpep_pickup_datetime":  "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "store_and_fwd_flag":    "store_and_fwd_flag",
    "RatecodeID":            "rate_code_id",
    "PULocationID":          "pickup_location_id",
    "DOLocationID":          "dropoff_location_id",
    "passenger_count":       "passenger_count",
    "trip_distance":         "trip_distance",
    "fare_amount":           "fare_amount",
    "extra":                 "extra_amount",
    "mta_tax":               "mta_tax",
    "tip_amount":            "tip_amount",
    "tolls_amount":          "tolls_amount",
    "ehail_fee":             "ehail_fee",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount":          "total_amount",
    "payment_type":          "payment_type",
    "trip_type":             "trip_type",
    "congestion_surcharge":  "congestion_surcharge",
}

FHVHV_COLUMN_MAP = {
    "hvfhs_license_num":    "vendor_id",
    "pickup_datetime":      "pickup_datetime",
    "dropoff_datetime":     "dropoff_datetime",
    "PULocationID":         "pickup_location_id",
    "DOLocationID":         "dropoff_location_id",
    "trip_miles":           "trip_distance",
    "base_passenger_fare":  "fare_amount",
    "tolls":                "tolls_amount",
    "tips":                 "tip_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee":          "airport_fee",
    "sales_tax":            "mta_tax",
}

SERVICE_COLUMN_MAPS = {
    "yellow": YELLOW_COLUMN_MAP,
    "green":  GREEN_COLUMN_MAP,
    "fhvhv":  FHVHV_COLUMN_MAP,
}


# ══════════════════════════════════════════════════════════════════
# 3. DIAGNÓSTICO DE ESQUEMA
# ══════════════════════════════════════════════════════════════════

def diagnose_schema(
    df:           DataFrame,
    service_type: str,
    metadata_dir: str,
    logger
) -> Dict[str, Any]:
    """
    Compara el esquema real del DataFrame contra el esquema esperado del JSON.

    Detecta:
      - Columnas faltantes (presentes en esperado, ausentes en real)
      - Columnas adicionales (presentes en real, ausentes en esperado)
      - Incompatibilidades de tipo (mismo nombre, tipo diferente)

    Returns:
        dict con el diagnóstico completo del archivo
    """
    # Cargar esquema esperado desde JSON
    schema_file = Path(metadata_dir) / f"expected_schema_{service_type}.json"
    if not schema_file.exists():
        logger.warning(f"  Sin esquema esperado para {service_type}, usando heurísticas.")
        return {"status": "NO_EXPECTED_SCHEMA", "issues": []}

    expected = load_metadata(str(schema_file))
    expected_fields = {f["name"]: f["type"] for f in expected["fields"]}

    actual_fields = {f.name: str(f.dataType).lower() for f in df.schema.fields}

    # Detectar diferencias
    missing_cols    = set(expected_fields.keys()) - set(actual_fields.keys())
    extra_cols      = set(actual_fields.keys()) - set(expected_fields.keys())
    type_mismatches = []

    for col_name in set(expected_fields.keys()) & set(actual_fields.keys()):
        expected_type = expected_fields[col_name].lower()
        actual_type   = actual_fields[col_name]
        if not _types_compatible(expected_type, actual_type):
            type_mismatches.append({
                "column": col_name,
                "expected": expected_type,
                "actual":   actual_type,
            })

    # Columnas críticas faltantes
    canon_map = SERVICE_COLUMN_MAPS.get(service_type, {})
    expected_critical = {
        orig for orig, canon in canon_map.items()
        if canon in CRITICAL_COLUMNS
    }
    missing_critical = missing_cols & expected_critical

    diagnosis = {
        "service_type":       service_type,
        "schema_hash":        compute_schema_hash(df.schema),
        "actual_columns":     len(actual_fields),
        "expected_columns":   len(expected_fields),
        "missing_columns":    sorted(missing_cols),
        "extra_columns":      sorted(extra_cols),
        "type_mismatches":    type_mismatches,
        "missing_critical":   sorted(missing_critical),
        "has_issues":         bool(missing_cols or type_mismatches),
        "is_recoverable":     len(missing_critical) == 0,
        "diagnosis_timestamp": get_current_timestamp(),
    }

    # Log del diagnóstico
    if diagnosis["has_issues"]:
        logger.warning(f"  Diagnóstico {service_type}: {len(missing_cols)} ausentes, "
                       f"{len(extra_cols)} extra, {len(type_mismatches)} tipo-incompatibles")
        if missing_critical:
            logger.error(f"  ¡COLUMNAS CRÍTICAS FALTANTES! {missing_critical}")
    else:
        logger.info(f"  Diagnóstico {service_type}: esquema OK")

    return diagnosis


def _types_compatible(expected: str, actual: str) -> bool:
    """Verifica si dos tipos de datos son compatibles (tolerante a aliases)."""
    type_aliases = {
        "long":      {"long", "bigint", "int64", "longtype()"},
        "double":    {"double", "float64", "doubletype()", "float"},
        "string":    {"string", "str", "stringtype()", "varchar"},
        "timestamp": {"timestamp", "timestamptype()", "datetime"},
        "integer":   {"integer", "int", "int32", "integertype()"},
        "boolean":   {"boolean", "bool", "booleantype()"},
    }
    for group in type_aliases.values():
        if expected in group and actual in group:
            return True
    return expected == actual


# ══════════════════════════════════════════════════════════════════
# 4. HOMOLOGACIÓN AL ESQUEMA CANÓNICO
# ══════════════════════════════════════════════════════════════════

def apply_canonical_mapping(
    df:           DataFrame,
    service_type: str,
    source_file:  str,
    process_id:   str,
    logger
) -> DataFrame:
    """
    Transforma un DataFrame al esquema canónico:

    1. Renombra columnas según la matriz de homologación del servicio
    2. Calcula total_amount para FHVHV (suma de componentes)
    3. Agrega columnas ausentes del esquema canónico con NULL controlado
    4. Fuerza tipos de datos correctos con manejo de errores
    5. Agrega metadatos: service_type, source_file, ingestion_timestamp
    """
    col_map = SERVICE_COLUMN_MAPS.get(service_type, {})

    # ── Paso 1: Renombrar columnas ─────────────────────────────────
    for original, canonical in col_map.items():
        if original in df.columns:
            df = df.withColumnRenamed(original, canonical)

    # ── Paso 2: Calcular total_amount para FHVHV ──────────────────
    if service_type == "fhvhv":
        df = _compute_fhvhv_total(df)

    # ── Paso 3: Conversión segura de tipos ─────────────────────────
    df = _safe_cast_columns(df)

    # ── Paso 4: Añadir columnas faltantes con NULL ─────────────────
    canonical_cols = set(CANONICAL_SCHEMA_FIELDS.keys()) - {
        "trip_id", "year", "month", "source_file",
        "ingestion_timestamp", "quality_status"
    }
    for col_name, col_type in CANONICAL_SCHEMA_FIELDS.items():
        if col_name not in df.columns and col_name in canonical_cols:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

    # ── Paso 5: Metadatos del pipeline ────────────────────────────
    df = (
        df
        .withColumn("service_type",        F.lit(service_type))
        .withColumn("source_file",         F.lit(source_file))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("quality_status",      F.lit("PENDING"))
        .withColumn("year",  F.year(F.col("pickup_datetime")).cast(IntegerType()))
        .withColumn("month", F.month(F.col("pickup_datetime")).cast(IntegerType()))
    )

    # Seleccionar solo columnas del esquema canónico en orden definido
    canonical_col_names = [c for c in CANONICAL_SCHEMA_FIELDS.keys() if c in df.columns]
    df = df.select(canonical_col_names)

    logger.debug(f"  Homologación {service_type}: {len(df.columns)} columnas canónicas")
    return df


def _compute_fhvhv_total(df: DataFrame) -> DataFrame:
    """
    Calcula total_amount para FHVHV como suma de todos los componentes de tarifa.
    Justificación: FHVHV no tiene campo total_amount nativo; los componentes
    son: tarifa base + peajes + BCF + impuesto ventas + recargo congestión +
    tarifa aeropuerto + propinas.
    """
    components = ["fare_amount", "tolls_amount", "mta_tax",
                  "congestion_surcharge", "airport_fee", "tip_amount"]
    # BCF es un cargo del fondo TLC, incluirlo en el total
    if "bcf" in df.columns:
        components.append("bcf")

    total_expr = sum(
        F.coalesce(F.col(c), F.lit(0.0))
        for c in components
        if c in df.columns
    )
    return df.withColumn("total_amount", total_expr)


def _safe_cast_columns(df: DataFrame) -> DataFrame:
    """
    Aplica conversiones de tipo seguras con manejo de errores.
    Si un valor no puede convertirse, resulta en NULL (no falla el pipeline).
    """
    cast_map = {
        "pickup_datetime":     TimestampType(),
        "dropoff_datetime":    TimestampType(),
        "pickup_location_id":  LongType(),
        "dropoff_location_id": LongType(),
        "fare_amount":         DoubleType(),
        "total_amount":        DoubleType(),
        "tip_amount":          DoubleType(),
        "tolls_amount":        DoubleType(),
        "congestion_surcharge": DoubleType(),
        "airport_fee":         DoubleType(),
        "mta_tax":             DoubleType(),
        "extra_amount":        DoubleType(),
        "trip_distance":       DoubleType(),
        "passenger_count":     DoubleType(),
        "payment_type":        LongType(),
        "vendor_id":           StringType(),
    }
    for col_name, target_type in cast_map.items():
        if col_name in df.columns:
            # try_cast devuelve NULL en lugar de error si el cast falla
            df = df.withColumn(
                col_name,
                F.col(col_name).cast(target_type)
            )
    return df


# ══════════════════════════════════════════════════════════════════
# 5. FASE DE DIAGNÓSTICO Y RECONSTRUCCIÓN COMPLETA
# ══════════════════════════════════════════════════════════════════

def run_schema_recovery_phase(
    spark:                SparkSession,
    dataframes_by_service: Dict[str, List[Tuple[str, DataFrame]]],
    config:               Dict[str, Any],
    process_id:           str,
    logger
) -> Tuple[Optional[DataFrame], List[Dict[str, Any]]]:
    """
    Fase 2: Diagnóstico de esquemas y reconstrucción al modelo canónico.

    Flujo:
      1. Para cada servicio y cada archivo: diagnosticar el esquema
      2. Si es recuperable: aplicar homologación canónica
      3. Si no es recuperable: enviar a cuarentena
      4. Unificar todos los DataFrames en uno consolidado (bronze)
      5. Guardar en capa bronze particionado por service_type/year/month

    Returns:
        (DataFrame unificado o None, lista de diagnósticos)
    """
    logger.info("═" * 70)
    logger.info(f"  FASE 2: DIAGNÓSTICO Y RECONSTRUCCIÓN  —  process_id: {process_id}")
    logger.info("═" * 70)

    metadata_dir  = config["paths"]["metadata_dir"]
    bronze_dir    = config["paths"]["bronze_dir"]
    quarantine_dir = config["paths"]["quarantine_dir"]

    all_dfs       = []
    diagnostics   = []
    stats         = {"total": 0, "recovered": 0, "quarantined": 0}

    for service_type, file_list in dataframes_by_service.items():
        if not file_list:
            continue

        logger.info(f"\n[2.1] Procesando servicio: {service_type.upper()} ({len(file_list)} archivos)")

        for file_path, df in file_list:
            stats["total"] += 1
            fname = Path(file_path).name

            # Diagnóstico del esquema
            diagnosis = diagnose_schema(df, service_type, metadata_dir, logger)
            diagnosis["file_name"] = fname
            diagnosis["process_id"] = process_id
            diagnostics.append(diagnosis)

            # Decidir acción según diagnóstico
            if not diagnosis.get("is_recoverable", True) and diagnosis.get("missing_critical"):
                # Archivo no recuperable por columnas críticas faltantes
                logger.error(f"  [QUAR] {fname} → no recuperable, enviando a cuarentena")
                _quarantine_bad_schema(fname, diagnosis, quarantine_dir, logger)
                stats["quarantined"] += 1
                continue

            # Aplicar homologación canónica
            try:
                canonical_df = apply_canonical_mapping(
                    df, service_type, fname, process_id, logger
                )
                all_dfs.append(canonical_df)
                stats["recovered"] += 1
                logger.info(f"  [OK]  {fname} → {canonical_df.count():,} registros reconstruidos")

            except Exception as e:
                logger.error(f"  [ERR] Falló homologación de {fname}: {e}")
                diagnosis["homologation_error"] = str(e)[:300]
                _quarantine_bad_schema(fname, diagnosis, quarantine_dir, logger)
                stats["quarantined"] += 1

    # ── Unificar DataFrames ────────────────────────────────────────
    if not all_dfs:
        logger.error("[2.2] ¡No hay DataFrames válidos para unificar!")
        return None, diagnostics

    logger.info(f"\n[2.2] Unificando {len(all_dfs)} DataFrames al esquema canónico...")
    bronze_df = all_dfs[0]
    for df in all_dfs[1:]:
        # unionByName tolera columnas en diferente orden
        bronze_df = bronze_df.unionByName(df, allowMissingColumns=True)

    # Optimización: coalesce para evitar archivos pequeños
    partitions = config["processing"].get("bronze_partitions", 4)
    bronze_df = bronze_df.coalesce(partitions)

    # ── Persistir capa bronze ──────────────────────────────────────
    import os
    from pathlib import Path as _Path

    # Convertir a ruta absoluta compatible Windows
    abs_bronze = str(_Path(bronze_dir).resolve())
    if os.name == "nt":
        spark_bronze = "file:///" + abs_bronze.replace("\\", "/")
    else:
        spark_bronze = abs_bronze

    _Path(abs_bronze).mkdir(parents=True, exist_ok=True)
    logger.info(f"[2.3] Guardando capa bronze en: {abs_bronze}")
    try:
        (
            bronze_df.coalesce(config["processing"].get("bronze_partitions", 4))
            .write
            .mode("overwrite")
            .partitionBy("service_type", "year", "month")
            .parquet(spark_bronze)
        )
    except Exception as e:
        logger.warning(f"  Particionado falló ({e}), reintentando sin partición...")
        (
            bronze_df.coalesce(2).write
            .mode("overwrite")
            .parquet(spark_bronze)
        )

    total_bronze = bronze_df.count()
    logger.info(f"\n[2.4] RESUMEN FASE 2 — DIAGNÓSTICO Y RECONSTRUCCIÓN:")
    logger.info(f"  Archivos procesados : {stats['total']}")
    logger.info(f"  Recuperados         : {stats['recovered']}")
    logger.info(f"  Cuarentena          : {stats['quarantined']}")
    logger.info(f"  Total registros     : {total_bronze:,}")
    logger.info("═" * 70)

    return bronze_df, diagnostics


def _quarantine_bad_schema(
    file_name:     str,
    diagnosis:     Dict[str, Any],
    quarantine_dir: str,
    logger
) -> None:
    """Registra un archivo con esquema inválido en la cuarentena."""
    import json
    qdir = Path(quarantine_dir) / "files"
    qdir.mkdir(parents=True, exist_ok=True)

    ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname  = file_name.replace(".parquet", "")
    entry  = {
        **diagnosis,
        "quarantine_reason":    "SCHEMA_NOT_RECOVERABLE_OR_HOMOLOGATION_FAILED",
        "quarantine_timestamp": get_current_timestamp(),
        "stage":               "SCHEMA_RECOVERY",
    }

    out_file = qdir / f"schema_rejected_{fname}_{ts}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2, default=str)

    logger.debug(f"  Diagnóstico guardado: {out_file.name}")


def read_bronze_layer(spark: SparkSession, bronze_dir: str, logger) -> Optional[DataFrame]:
    """
    Lee la capa bronze desde disco (útil para notebooks independientes).
    Aplica partition pruning automático al leer.
    """
    bronze_path = Path(bronze_dir)
    if not bronze_path.exists() or not any(bronze_path.iterdir()):
        logger.error(f"Capa bronze vacía o no encontrada: {bronze_dir}")
        return None

    df = (
        spark.read
        .option("mergeSchema", "true")   # Bronze puede tener variaciones leves
        .parquet(bronze_dir)
    )
    logger.info(f"Capa bronze cargada: {df.count():,} registros, {len(df.columns)} columnas")
    return df
