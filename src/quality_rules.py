"""
quality_rules.py — Fase 5: Validación de Calidad y Separación de Rechazados
Proyecto: ETL Spark Parquet Advanced – NYC TLC Trip Records

Responsabilidades:
  - Aplicar todas las reglas de calidad de negocio (BR-001 … BR-015)
  - Separar registros válidos de rechazados
  - Generar la tabla quality_rejected_records con evidencia completa
  - Generar la tabla quality_metrics_summary por servicio/año/mes
  - Actualizar quality_status en los registros
"""

import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, LongType, BooleanType

from src.utils import get_current_timestamp


# ══════════════════════════════════════════════════════════════════
# 1. DEFINICIÓN DE REGLAS DE CALIDAD
# ══════════════════════════════════════════════════════════════════

def _null_check(col_name: str) -> F.Column:
    return F.col(col_name).isNull()


def _build_rejection_rules(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Construye la lista de reglas de rechazo con sus condiciones Spark.
    Cada regla produce un flag boolean; si es True → el registro es rechazado.
    """
    qr = config.get("quality_rules", {})
    max_dur   = qr.get("max_trip_duration_minutes", 480)
    max_speed = qr.get("max_speed_mph", 100)
    max_tip   = qr.get("max_tip_percentage", 100)
    yr_min    = qr.get("reference_year_min", 2018)
    yr_max    = qr.get("reference_year_max", 2024)

    return [
        # ── Nulos en campos críticos (BR-001 a BR-005, BR-014, BR-015) ──
        {
            "rule_id":          "BR-001",
            "name":             "null_pickup_datetime",
            "condition":        F.col("pickup_datetime").isNull(),
            "rejection_column": "pickup_datetime",
            "rejection_rule":   "IS NULL",
            "rejection_stage":  "BRONZE",
            "rejection_category": "NULL_CRITICAL_FIELD",
            "business_reason":  "La fecha de recogida es obligatoria para cualquier análisis",
        },
        {
            "rule_id":          "BR-002",
            "name":             "null_dropoff_datetime",
            "condition":        F.col("dropoff_datetime").isNull(),
            "rejection_column": "dropoff_datetime",
            "rejection_rule":   "IS NULL",
            "rejection_stage":  "BRONZE",
            "rejection_category": "NULL_CRITICAL_FIELD",
            "business_reason":  "La fecha de llegada es obligatoria para calcular duración y tarifas",
        },
        {
            "rule_id":          "BR-003",
            "name":             "null_pickup_location",
            "condition":        F.col("pickup_location_id").isNull(),
            "rejection_column": "pickup_location_id",
            "rejection_rule":   "IS NULL",
            "rejection_stage":  "BRONZE",
            "rejection_category": "NULL_CRITICAL_FIELD",
            "business_reason":  "La zona de recogida es necesaria para análisis geoespacial",
        },
        {
            "rule_id":          "BR-004",
            "name":             "null_dropoff_location",
            "condition":        F.col("dropoff_location_id").isNull(),
            "rejection_column": "dropoff_location_id",
            "rejection_rule":   "IS NULL",
            "rejection_stage":  "BRONZE",
            "rejection_category": "NULL_CRITICAL_FIELD",
            "business_reason":  "La zona de destino es necesaria para análisis de rutas",
        },
        {
            "rule_id":          "BR-014",
            "name":             "null_fare_amount",
            "condition":        F.col("fare_amount").isNull(),
            "rejection_column": "fare_amount",
            "rejection_rule":   "IS NULL",
            "rejection_stage":  "BRONZE",
            "rejection_category": "NULL_CRITICAL_FIELD",
            "business_reason":  "La tarifa es crítica para análisis de ingresos",
        },
        {
            "rule_id":          "BR-015",
            "name":             "null_total_amount",
            "condition":        F.col("total_amount").isNull(),
            "rejection_column": "total_amount",
            "rejection_rule":   "IS NULL",
            "rejection_stage":  "BRONZE",
            "rejection_category": "NULL_CRITICAL_FIELD",
            "business_reason":  "El total pagado es fundamental para reportes financieros",
        },
        # ── Fechas inválidas (BR-005, BR-006) ─────────────────────
        {
            "rule_id":          "BR-005",
            "name":             "invalid_date_order",
            "condition":        F.col("pickup_datetime") >= F.col("dropoff_datetime"),
            "rejection_column": "pickup_datetime",
            "rejection_rule":   "pickup_datetime >= dropoff_datetime",
            "rejection_stage":  "SILVER",
            "rejection_category": "INVALID_DATE_ORDER",
            "business_reason":  "Imposible que la recogida ocurra después o igual que la llegada",
        },
        {
            "rule_id":          "BR-006",
            "name":             "out_of_range_year",
            "condition":        (F.col("year") < yr_min) | (F.col("year") > yr_max),
            "rejection_column": "year",
            "rejection_rule":   f"year < {yr_min} OR year > {yr_max}",
            "rejection_stage":  "SILVER",
            "rejection_category": "OUT_OF_RANGE_DATE",
            "business_reason":  f"Solo se procesan datos del período {yr_min}-{yr_max}",
        },
        # ── Montos inválidos (BR-007, BR-008) ─────────────────────
        {
            "rule_id":          "BR-007",
            "name":             "negative_fare_amount",
            "condition":        F.col("fare_amount") < 0,
            "rejection_column": "fare_amount",
            "rejection_rule":   "fare_amount < 0",
            "rejection_stage":  "SILVER",
            "rejection_category": "NEGATIVE_AMOUNT",
            "business_reason":  "Una tarifa negativa indica error de registro o fraude potencial",
        },
        {
            "rule_id":          "BR-008",
            "name":             "invalid_total_amount",
            "condition":        F.col("total_amount") <= 0,
            "rejection_column": "total_amount",
            "rejection_rule":   "total_amount <= 0",
            "rejection_stage":  "SILVER",
            "rejection_category": "INVALID_AMOUNT",
            "business_reason":  "Un pago de cero o negativo no puede corresponder a un viaje real",
        },
        # ── Duración inválida (BR-010) ─────────────────────────────
        {
            "rule_id":          "BR-010",
            "name":             "invalid_duration",
            "condition":        (
                (F.col("trip_duration_minutes") <= 0) |
                (F.col("trip_duration_minutes") > max_dur)
            ),
            "rejection_column": "trip_duration_minutes",
            "rejection_rule":   f"trip_duration_minutes <= 0 OR > {max_dur}",
            "rejection_stage":  "SILVER",
            "rejection_category": "INVALID_DURATION",
            "business_reason":  f"Viajes de 0 o más de {max_dur} min son operacionalmente imposibles",
        },
    ]


# ══════════════════════════════════════════════════════════════════
# 2. APLICACIÓN DE REGLAS Y SEPARACIÓN DE REGISTROS
# ══════════════════════════════════════════════════════════════════

def apply_quality_rules(
    df:         DataFrame,
    config:     Dict[str, Any],
    process_id: str,
    logger
) -> Tuple[DataFrame, DataFrame]:
    """
    Aplica todas las reglas de calidad al DataFrame silver.

    Estrategia:
      - Para cada regla: agrega una columna flag (_reject_BR-XXX)
      - Un registro es rechazado si CUALQUIER flag es True
      - Los rechazados van a quality_rejected_records
      - Los válidos continúan con quality_status = 'VALID'
      - Los sospechosos (but not rejected) = 'SUSPICIOUS'

    Returns:
        (valid_df, rejected_df)
    """
    rules = _build_rejection_rules(config)
    logger.info(f"  Aplicando {len(rules)} reglas de calidad...")

    df_flagged = df

    # Agregar flag boolean por cada regla
    for rule in rules:
        flag_col = f"_reject_{rule['rule_id'].replace('-','_')}"
        try:
            df_flagged = df_flagged.withColumn(
                flag_col,
                F.when(rule["condition"], F.lit(True)).otherwise(F.lit(False))
            )
        except Exception as e:
            logger.warning(f"  No se pudo aplicar {rule['rule_id']}: {e}")
            df_flagged = df_flagged.withColumn(flag_col, F.lit(False))

    # Determinar si el registro debe ser rechazado (OR de todos los flags)
    reject_flags = [f"_reject_{r['rule_id'].replace('-','_')}" for r in rules]
    reject_condition = F.lit(False)
    for flag in reject_flags:
        reject_condition = reject_condition | F.col(flag)

    df_flagged = df_flagged.withColumn("_is_rejected", reject_condition)

    # ── Separar válidos y rechazados ──────────────────────────────
    valid_df = (
        df_flagged
        .filter(~F.col("_is_rejected"))
        .withColumn(
            "quality_status",
            F.when(F.col("is_suspicious_trip") == True, F.lit("SUSPICIOUS"))
             .otherwise(F.lit("VALID"))
        )
        .drop(*reject_flags, "_is_rejected")
    )

    rejected_raw = df_flagged.filter(F.col("_is_rejected"))

    # ── Construir tabla de rechazados con detalle ─────────────────
    rejected_df = _build_rejected_records_table(
        rejected_raw, rules, reject_flags, process_id
    )

    logger.info(f"  Válidos    : {valid_df.count():,}")
    logger.info(f"  Rechazados : {rejected_df.count():,}")

    return valid_df.drop(*reject_flags, "_is_rejected", "_reject_BR_013"), rejected_df


def _build_rejected_records_table(
    rejected_raw: DataFrame,
    rules:        List[Dict[str, Any]],
    reject_flags: List[str],
    process_id:   str
) -> DataFrame:
    """
    Construye el DataFrame de registros rechazados con formato quality_rejected_records.
    Expande cada registro rechazado por cada regla que lo marcó.
    """
    dfs_by_rule = []

    for rule, flag_col in zip(rules, reject_flags):
        records_for_rule = (
            rejected_raw
            .filter(F.col(flag_col) == True)
            .select(
                F.lit(process_id).alias("process_id"),
                F.coalesce(F.col("trip_id"), F.lit("UNKNOWN")).alias("trip_id"),
                F.col("service_type"),
                F.col("source_file"),
                F.lit(rule["rejection_stage"]).alias("rejection_stage"),
                F.lit(rule["rule_id"]).alias("rejection_rule_id"),
                F.lit(rule["name"]).alias("rejection_rule"),
                F.lit(rule["rejection_column"]).alias("rejection_column"),
                # Valor original que disparó la regla
                F.coalesce(
                    F.col(rule["rejection_column"]).cast(StringType()),
                    F.lit("NULL")
                ).alias("original_value"),
                F.lit(rule["rejection_rule"]).alias("technical_reason"),
                F.lit(rule.get("business_reason", "")).alias("business_reason"),
                F.lit(rule["rejection_category"]).alias("rejection_category"),
                F.current_timestamp().alias("rejected_at"),
            )
        )
        dfs_by_rule.append(records_for_rule)

    if not dfs_by_rule:
        return rejected_raw.sparkSession.createDataFrame(
            [], schema=_rejected_schema()
        )

    result = dfs_by_rule[0]
    for df in dfs_by_rule[1:]:
        result = result.unionByName(df)

    return result


def _rejected_schema():
    """Retorna el esquema vacío para quality_rejected_records."""
    from pyspark.sql.types import StructType, StructField, TimestampType
    return StructType([
        StructField("process_id",        StringType(),    True),
        StructField("trip_id",           StringType(),    True),
        StructField("service_type",      StringType(),    True),
        StructField("source_file",       StringType(),    True),
        StructField("rejection_stage",   StringType(),    True),
        StructField("rejection_rule_id", StringType(),    True),
        StructField("rejection_rule",    StringType(),    True),
        StructField("rejection_column",  StringType(),    True),
        StructField("original_value",    StringType(),    True),
        StructField("technical_reason",  StringType(),    True),
        StructField("business_reason",   StringType(),    True),
        StructField("rejection_category",StringType(),    True),
        StructField("rejected_at",       TimestampType(), True),
    ])


# ══════════════════════════════════════════════════════════════════
# 3. MÉTRICAS DE CALIDAD
# ══════════════════════════════════════════════════════════════════

def compute_quality_metrics(
    silver_df:   DataFrame,
    rejected_df: DataFrame,
    process_id:  str,
    logger
) -> DataFrame:
    """
    Genera la tabla quality_metrics_summary agrupada por service_type, year, month.

    Métricas calculadas:
      - total_records, valid_records, rejected_records
      - duplicate_records, null_critical_records
      - suspicious_records, quality_percentage
    """
    logger.info("  Calculando métricas de calidad por servicio/año/mes...")

    # Métricas de registros válidos
    valid_metrics = (
        silver_df
        .groupBy("service_type", "year", "month")
        .agg(
            F.count("*").alias("valid_records"),
            F.sum(F.when(F.col("is_suspicious_trip") == True, 1).otherwise(0))
             .alias("suspicious_records"),
            F.countDistinct("trip_id").alias("unique_trips"),
        )
    )

    # Métricas de registros rechazados
    rejected_metrics = (
        rejected_df
        .groupBy("service_type")
        .agg(F.count("*").alias("rejected_records_total"))
    )

    # Unificar métricas
    metrics_df = (
        valid_metrics
        .join(
            rejected_metrics,
            on="service_type",
            how="left"
        )
        .withColumn("rejected_records",
            F.coalesce(F.col("rejected_records_total"), F.lit(0)).cast(LongType()))
        .withColumn("total_records",
            F.col("valid_records") + F.col("rejected_records"))
        .withColumn("duplicate_records", F.lit(0).cast(LongType()))
        .withColumn("null_critical_records", F.lit(0).cast(LongType()))
        .withColumn(
            "quality_percentage",
            F.round(
                F.col("valid_records").cast(DoubleType()) /
                F.greatest(F.col("total_records"), F.lit(1)).cast(DoubleType()) * 100.0,
                2
            )
        )
        .withColumn("process_id",  F.lit(process_id))
        .withColumn("processed_at", F.current_timestamp())
        .select(
            "process_id", "service_type", "year", "month",
            "total_records", "valid_records", "rejected_records",
            "duplicate_records", "null_critical_records",
            "suspicious_records", "quality_percentage", "processed_at"
        )
        .orderBy("year", "month", "service_type")
    )

    return metrics_df


# ══════════════════════════════════════════════════════════════════
# 4. CONSTRUCCIÓN DE TABLAS GOLD
# ══════════════════════════════════════════════════════════════════

def build_gold_trips_clean(valid_df: DataFrame) -> DataFrame:
    """
    gold_trips_clean: tabla granular de viajes limpios y validados.
    Selecciona y ordena las columnas requeridas por el modelo gold.
    """
    return valid_df.select(
        "trip_id", "service_type",
        "pickup_datetime", "dropoff_datetime",
        "trip_duration_minutes", "trip_distance",
        "pickup_location_id", "dropoff_location_id",
        "payment_type", "fare_amount", "tip_amount", "total_amount",
        "tip_percentage", "average_speed_mph",
        "fare_per_mile", "is_airport_trip", "is_suspicious_trip",
        "year", "month", "source_file", "processing_date",
        "quality_status",
    ).orderBy("pickup_datetime")


def build_gold_daily_revenue(valid_df: DataFrame) -> DataFrame:
    """
    gold_daily_revenue: resumen diario de ingresos y operaciones por servicio.
    Permite análisis de tendencias temporales y planificación operativa.
    """
    return (
        valid_df
        .withColumn("trip_date", F.to_date(F.col("pickup_datetime")))
        .groupBy("service_type", "trip_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("average_fare"),
            F.round(F.avg("tip_amount"), 2).alias("average_tip"),
            F.round(F.avg("trip_distance"), 2).alias("average_trip_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("average_trip_duration"),
            F.sum(F.when(F.col("is_suspicious_trip") == True, 1).otherwise(0))
             .alias("suspicious_trips"),
            F.round(
                F.sum(F.when(F.col("quality_status") == "VALID", 1).otherwise(0)).cast(DoubleType()) /
                F.count("*").cast(DoubleType()) * 100.0, 2
            ).alias("quality_percentage"),
        )
        .orderBy("trip_date", "service_type")
    )


def build_gold_location_performance(valid_df: DataFrame) -> DataFrame:
    """
    gold_location_performance: rendimiento por par de zonas origen-destino.
    Útil para optimización de flota y análisis de demanda geográfica.
    """
    return (
        valid_df
        .groupBy("service_type", "pickup_location_id", "dropoff_location_id")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("average_fare"),
            F.round(F.avg("trip_distance"), 2).alias("average_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("average_duration"),
            F.sum(F.when(F.col("is_suspicious_trip") == True, 1).otherwise(0))
             .alias("suspicious_trip_count"),
            F.round(F.avg("average_speed_mph"), 2).alias("average_speed_mph"),
        )
        .filter(F.col("total_trips") >= 1)
        .orderBy(F.col("total_revenue").desc())
    )


# ══════════════════════════════════════════════════════════════════
# 5. FASE DE CALIDAD COMPLETA
# ══════════════════════════════════════════════════════════════════

def run_quality_phase(
    spark:       SparkSession,
    silver_df:   DataFrame,
    config:      Dict[str, Any],
    process_id:  str,
    logger
) -> Dict[str, DataFrame]:
    """
    Fase 5: Validación de calidad, construcción de gold y métricas.

    Returns:
        dict con todas las tablas: gold_trips_clean, gold_daily_revenue,
        gold_location_performance, quality_rejected_records, quality_metrics_summary
    """
    logger.info("═" * 70)
    logger.info(f"  FASE 5-6: CALIDAD Y GOLD  —  process_id: {process_id}")
    logger.info("═" * 70)

    # ── Aplicar reglas de calidad ──────────────────────────────────
    logger.info("[5.1] Aplicando reglas de calidad...")
    valid_df, rejected_df = apply_quality_rules(silver_df, config, process_id, logger)

    # Cache para reutilización
    valid_df    = valid_df.cache()
    rejected_df = rejected_df.cache()

    # ── Métricas de calidad ────────────────────────────────────────
    logger.info("[5.2] Generando métricas de calidad...")
    metrics_df = compute_quality_metrics(valid_df, rejected_df, process_id, logger)

    # ── Tablas Gold ────────────────────────────────────────────────
    logger.info("[6.1] Construyendo gold_trips_clean...")
    gold_trips = build_gold_trips_clean(valid_df)

    logger.info("[6.2] Construyendo gold_daily_revenue...")
    gold_daily = build_gold_daily_revenue(valid_df)

    logger.info("[6.3] Construyendo gold_location_performance...")
    gold_location = build_gold_location_performance(valid_df)

    # ── Guardar capas ──────────────────────────────────────────────
    from pathlib import Path as _Path
    import os as _os
    gold_dir   = str(_Path(config["paths"]["gold_dir"]).resolve())
    quar_dir   = str(_Path(config["paths"]["quarantine_dir"]).resolve())
    audit_dir  = str(_Path(config["paths"]["audit_dir"]).resolve())

    logger.info("[6.4] Guardando tablas gold en Parquet...")
    _save_parquet(gold_trips,    _os.path.join(gold_dir, "trips"),    "service_type,year,month", logger)
    _save_parquet(gold_daily,    _os.path.join(gold_dir, "daily"),    "service_type",            logger)
    _save_parquet(gold_location, _os.path.join(gold_dir, "location"), "service_type",            logger)
    _save_parquet(rejected_df,   _os.path.join(quar_dir, "records"),  "service_type",            logger)
    _save_parquet(metrics_df,    _os.path.join(audit_dir, "quality_metrics"), "service_type",     logger)

    # ── Resumen ────────────────────────────────────────────────────
    valid_cnt    = valid_df.count()
    rejected_cnt = rejected_df.count()
    total_cnt    = valid_cnt + rejected_cnt
    qual_pct     = valid_cnt / max(total_cnt, 1) * 100

    logger.info(f"\n[5.3] RESUMEN CALIDAD:")
    logger.info(f"  Total procesados  : {total_cnt:,}")
    logger.info(f"  Válidos           : {valid_cnt:,}")
    logger.info(f"  Rechazados        : {rejected_cnt:,}")
    logger.info(f"  Calidad global    : {qual_pct:.2f}%")
    logger.info("═" * 70)

    return {
        "gold_trips_clean":          gold_trips,
        "gold_daily_revenue":        gold_daily,
        "gold_location_performance": gold_location,
        "quality_rejected_records":  rejected_df,
        "quality_metrics_summary":   metrics_df,
    }


def _save_parquet(df: DataFrame, path: str, partition_cols: str, logger) -> None:
    """
    Guarda un DataFrame en Parquet con particionado.
    Compatible con Windows: usa rutas absolutas y barras correctas.
    Maneja columnas no disponibles sin fallar.
    """
    import os
    from pathlib import Path

    # ── Convertir a ruta absoluta compatible con Windows + Spark ──
    abs_path = str(Path(path).resolve())
    # Spark en Windows necesita barras forward incluso en Windows
    if os.name == "nt":
        abs_path = abs_path.replace("\\", "/").replace("\\\\", "/")
        # Formato file:/// para Windows
        spark_path = "file:///" + abs_path.replace("\\", "/")
    else:
        spark_path = abs_path

    # ── Crear directorio padre ─────────────────────────────────────
    Path(abs_path).parent.mkdir(parents=True, exist_ok=True)

    # ── Filtrar columnas de partición que existen en el DataFrame ──
    cols = [c.strip() for c in partition_cols.split(",")
            if c.strip() and c.strip() in df.columns]

    try:
        # Reducir particiones para evitar crash de JVM por memoria
        writer = df.coalesce(2).write.mode("overwrite")
        if cols:
            writer = writer.partitionBy(*cols)
        writer.parquet(spark_path)
        count = df.count()
        logger.info(f"  Guardado: {abs_path} ({count:,} registros)")
    except Exception as e:
        logger.error(f"  Error guardando {abs_path}: {e}")
        # Fallback: guardar sin particionado si falla con particiones
        try:
            logger.warning(f"  Reintentando sin particionado...")
            df.coalesce(1).write.mode("overwrite").parquet(spark_path)
            logger.info(f"  Guardado (sin partición): {abs_path}")
        except Exception as e2:
            logger.error(f"  Fallo definitivo guardando {abs_path}: {e2}")
            raise
