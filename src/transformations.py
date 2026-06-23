"""
transformations.py — Fase 4: Transformación y Enriquecimiento de Datos
Proyecto: ETL Spark Parquet Advanced – NYC TLC Trip Records

Responsabilidades:
  - Normalizar columnas y tipos de datos
  - Calcular métricas derivadas: duración, velocidad, tarifa/milla, propina %
  - Generar trip_id único mediante hash SHA-256
  - Detectar y marcar viajes sospechosos
  - Eliminar duplicados técnicos
  - Enriquecer con campos de partición y metadatos
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, BooleanType, LongType
)

from src.utils import get_current_timestamp


# ══════════════════════════════════════════════════════════════════
# 1. CONSTANTES DE NEGOCIO
# ══════════════════════════════════════════════════════════════════

# IDs de zona de aeropuerto según NYC TLC (JFK=132, LGA=138, EWR=1)
AIRPORT_LOCATION_IDS = [1, 132, 138]

# Umbrales para viajes sospechosos
MAX_SPEED_MPH        = 100.0
MAX_DURATION_MINUTES = 480.0
MAX_TIP_PERCENTAGE   = 100.0

# Tipos de pago válidos según NYC TLC
VALID_PAYMENT_TYPES  = {1, 2, 3, 4, 5, 6}


# ══════════════════════════════════════════════════════════════════
# 2. GENERACIÓN DE TRIP_ID
# ══════════════════════════════════════════════════════════════════

def add_trip_id(df: DataFrame) -> DataFrame:
    """
    Genera un identificador único por viaje usando SHA-256 sobre campos clave.

    La clave de unicidad incluye: service_type + pickup_datetime + dropoff_datetime
    + pickup_location_id + dropoff_location_id + fare_amount + source_file

    Esto garantiza:
      - Idempotencia: mismo viaje siempre genera el mismo ID
      - Unicidad: dos viajes distintos generan IDs diferentes
      - Trazabilidad: el ID es reproducible sin secuencia externa
    """
    key_expr = F.concat_ws(
        "|",
        F.coalesce(F.col("service_type"),          F.lit("")),
        F.coalesce(F.col("pickup_datetime").cast(StringType()),  F.lit("")),
        F.coalesce(F.col("dropoff_datetime").cast(StringType()), F.lit("")),
        F.coalesce(F.col("pickup_location_id").cast(StringType()),  F.lit("")),
        F.coalesce(F.col("dropoff_location_id").cast(StringType()), F.lit("")),
        F.coalesce(F.col("fare_amount").cast(StringType()),  F.lit("")),
        F.coalesce(F.col("source_file"), F.lit("")),
    )
    return df.withColumn("trip_id", F.sha2(key_expr, 256))


# ══════════════════════════════════════════════════════════════════
# 3. MÉTRICAS DERIVADAS
# ══════════════════════════════════════════════════════════════════

def add_trip_duration(df: DataFrame) -> DataFrame:
    """
    Calcula la duración del viaje en minutos.
    Resultado negativo o cero indica fechas inválidas (será marcado sospechoso).
    """
    duration_expr = (
        F.col("dropoff_datetime").cast("long") -
        F.col("pickup_datetime").cast("long")
    ) / 60.0  # segundos → minutos

    return df.withColumn(
        "trip_duration_minutes",
        F.round(duration_expr, 2).cast(DoubleType())
    )


def add_average_speed(df: DataFrame) -> DataFrame:
    """
    Calcula la velocidad promedio en millas por hora (mph).
    Fórmula: distancia / (duración_en_horas)
    NULL cuando la duración es 0 o negativa para evitar división por cero.
    """
    speed_expr = F.when(
        (F.col("trip_duration_minutes") > 0) & (F.col("trip_distance") > 0),
        F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)
    ).otherwise(F.lit(None).cast(DoubleType()))

    return df.withColumn(
        "average_speed_mph",
        F.round(speed_expr, 2).cast(DoubleType())
    )


def add_fare_per_mile(df: DataFrame) -> DataFrame:
    """
    Calcula la tarifa por milla.
    NULL cuando la distancia es 0 o negativa.
    """
    fare_per_mile_expr = F.when(
        F.col("trip_distance") > 0,
        F.col("fare_amount") / F.col("trip_distance")
    ).otherwise(F.lit(None).cast(DoubleType()))

    return df.withColumn(
        "fare_per_mile",
        F.round(fare_per_mile_expr, 4).cast(DoubleType())
    )


def add_tip_percentage(df: DataFrame) -> DataFrame:
    """
    Calcula el porcentaje de propina sobre la tarifa base.
    NULL cuando la tarifa es 0 o negativa para evitar división por cero.
    """
    tip_pct_expr = F.when(
        F.col("fare_amount") > 0,
        (F.coalesce(F.col("tip_amount"), F.lit(0.0)) / F.col("fare_amount")) * 100.0
    ).otherwise(F.lit(None).cast(DoubleType()))

    return df.withColumn(
        "tip_percentage",
        F.round(tip_pct_expr, 2).cast(DoubleType())
    )


def add_is_airport_trip(df: DataFrame) -> DataFrame:
    """
    Marca viajes con origen o destino en aeropuertos JFK (132), LGA (138) o EWR (1).
    Esta información es útil para análisis de rutas premium.
    """
    airport_ids = AIRPORT_LOCATION_IDS
    return df.withColumn(
        "is_airport_trip",
        (
            F.col("pickup_location_id").isin(airport_ids) |
            F.col("dropoff_location_id").isin(airport_ids)
        ).cast(BooleanType())
    )


def add_processing_date(df: DataFrame) -> DataFrame:
    """Agrega la fecha de procesamiento del registro en el pipeline."""
    return df.withColumn(
        "processing_date",
        F.current_date()
    )


# ══════════════════════════════════════════════════════════════════
# 4. DETECCIÓN DE VIAJES SOSPECHOSOS
# ══════════════════════════════════════════════════════════════════

def add_suspicious_flag(df: DataFrame, config: Dict[str, Any] = None) -> DataFrame:
    """
    Marca viajes como sospechosos si cumplen alguna de las condiciones anómalas.

    Condiciones de sospecha (según reglas de negocio):
      1. Distancia <= 0
      2. Total pagado <= 0
      3. Tarifa negativa
      4. Duración <= 0 minutos
      5. Duración > 480 minutos (8 horas)
      6. Velocidad > 100 mph (imposible en NYC)
      7. Propina > 100% de la tarifa
      8. pickup_datetime >= dropoff_datetime

    Importante: Marcar como sospechoso NO excluye el registro del dataset,
    permite análisis posterior y transparencia en la calidad de datos.
    """
    max_speed    = MAX_SPEED_MPH
    max_duration = MAX_DURATION_MINUTES
    max_tip_pct  = MAX_TIP_PERCENTAGE

    if config:
        qr = config.get("quality_rules", {})
        max_speed    = qr.get("max_speed_mph",        max_speed)
        max_duration = qr.get("max_trip_duration_minutes", max_duration)
        max_tip_pct  = qr.get("max_tip_percentage",   max_tip_pct)

    suspicious_expr = (
        (F.col("trip_distance") <= 0)                              |  # BR-009
        (F.col("total_amount") <= 0)                               |  # BR-008
        (F.col("fare_amount") < 0)                                 |  # BR-007
        (F.col("trip_duration_minutes") <= 0)                      |  # BR-010
        (F.col("trip_duration_minutes") > max_duration)            |  # BR-010
        (F.col("average_speed_mph") > max_speed)                   |  # BR-011
        (F.col("tip_percentage") > max_tip_pct)                    |  # BR-012
        (F.col("pickup_datetime") >= F.col("dropoff_datetime"))       # BR-005
    )

    return df.withColumn(
        "is_suspicious_trip",
        suspicious_expr.cast(BooleanType())
    )


# ══════════════════════════════════════════════════════════════════
# 5. DEDUPLICACIÓN
# ══════════════════════════════════════════════════════════════════

def remove_technical_duplicates(df: DataFrame, logger) -> Tuple[DataFrame, int]:
    """
    Elimina duplicados técnicos usando trip_id como clave de unicidad.

    Estrategia de desempate: se conserva el registro con la ingestion_timestamp
    más temprana (el primer procesamiento de ese viaje).

    Returns:
        (DataFrame deduplicado, número de duplicados eliminados)
    """
    from pyspark.sql.window import Window

    total_before = df.count()

    # Ordenar por ingestion_timestamp ASC para conservar el primero
    window = Window.partitionBy("trip_id").orderBy(F.col("ingestion_timestamp").asc())
    df_dedup = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    total_after = df_dedup.count()
    duplicates_removed = total_before - total_after

    if duplicates_removed > 0:
        logger.warning(f"  Duplicados eliminados: {duplicates_removed:,} "
                       f"({duplicates_removed / total_before * 100:.2f}% del dataset)")
    else:
        logger.info("  Sin duplicados técnicos detectados")

    return df_dedup, duplicates_removed


# ══════════════════════════════════════════════════════════════════
# 6. NORMALIZACIÓN DE PARTICIONES
# ══════════════════════════════════════════════════════════════════

def fix_year_month_columns(df: DataFrame) -> DataFrame:
    """
    Recalcula year y month directamente desde pickup_datetime (fuente de verdad).
    Detecta inconsistencias entre la partición del archivo y la fecha real del viaje.
    """
    return (
        df
        .withColumn("year",  F.year(F.col("pickup_datetime")).cast(IntegerType()))
        .withColumn("month", F.month(F.col("pickup_datetime")).cast(IntegerType()))
    )


def normalize_amounts(df: DataFrame) -> DataFrame:
    """
    Normaliza campos monetarios: convierte valores nulos a 0.0 cuando aplica.
    Solo para campos no críticos (extra, mta_tax, etc.), los críticos mantienen NULL.
    """
    optional_amounts = [
        "extra_amount", "mta_tax", "improvement_surcharge",
        "congestion_surcharge", "ehail_fee"
    ]
    for col_name in optional_amounts:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.coalesce(F.col(col_name), F.lit(0.0)).cast(DoubleType())
            )
    return df


def normalize_strings(df: DataFrame) -> DataFrame:
    """
    Normaliza campos string: eliminar espacios, convertir a minúsculas donde aplica.
    """
    if "store_and_fwd_flag" in df.columns:
        df = df.withColumn(
            "store_and_fwd_flag",
            F.upper(F.trim(F.col("store_and_fwd_flag")))
        )
    if "quality_status" in df.columns:
        df = df.withColumn(
            "quality_status",
            F.upper(F.trim(F.col("quality_status")))
        )
    return df


# ══════════════════════════════════════════════════════════════════
# 7. PIPELINE DE TRANSFORMACIÓN COMPLETA
# ══════════════════════════════════════════════════════════════════

def run_transformation_phase(
    spark:      SparkSession,
    bronze_df:  DataFrame,
    config:     Dict[str, Any],
    process_id: str,
    logger
) -> DataFrame:
    """
    Fase 4: Aplica todas las transformaciones al DataFrame bronze.

    Transformaciones en orden:
      1.  Normalización de strings
      2.  Normalización de montos opcionales
      3.  Recálculo de year/month desde fechas reales
      4.  Generación de trip_id (SHA-256)
      5.  Cálculo de duración del viaje
      6.  Cálculo de velocidad promedio
      7.  Cálculo de tarifa por milla
      8.  Cálculo de porcentaje de propina
      9.  Indicador de viaje aeroportuario
      10. Indicador de viaje sospechoso
      11. Deduplicación técnica
      12. Campo processing_date
      13. Coalesce para manejo de archivos pequeños

    Returns:
        DataFrame transformado (pre-validación de calidad)
    """
    logger.info("═" * 70)
    logger.info(f"  FASE 4: TRANSFORMACIÓN  —  process_id: {process_id}")
    logger.info("═" * 70)

    initial_count = bronze_df.count()
    logger.info(f"[4.0] Registros entrantes: {initial_count:,}")

    # Transformaciones
    logger.info("[4.1]  Normalizando strings y montos...")
    df = normalize_strings(bronze_df)
    df = normalize_amounts(df)

    logger.info("[4.2]  Recalculando particiones year/month desde fechas reales...")
    df = fix_year_month_columns(df)

    logger.info("[4.3]  Generando trip_id (SHA-256)...")
    df = add_trip_id(df)

    logger.info("[4.4]  Calculando duración del viaje (minutos)...")
    df = add_trip_duration(df)

    logger.info("[4.5]  Calculando velocidad promedio (mph)...")
    df = add_average_speed(df)

    logger.info("[4.6]  Calculando tarifa por milla...")
    df = add_fare_per_mile(df)

    logger.info("[4.7]  Calculando porcentaje de propina...")
    df = add_tip_percentage(df)

    logger.info("[4.8]  Marcando viajes aeroportuarios...")
    df = add_is_airport_trip(df)

    logger.info("[4.9]  Marcando viajes sospechosos...")
    df = add_suspicious_flag(df, config)

    logger.info("[4.10] Añadiendo processing_date...")
    df = add_processing_date(df)

    logger.info("[4.11] Eliminando duplicados técnicos...")
    df, dups_removed = remove_technical_duplicates(df, logger)

    # Optimización: coalesce para evitar archivos pequeños en silver
    partitions = config["processing"].get("silver_partitions", 16)
    df = df.repartition(partitions, "service_type", "year", "month")
    df = df.coalesce(partitions)

    # Estadísticas finales
    final_count   = df.count()
    suspicious    = df.filter(F.col("is_suspicious_trip") == True).count()
    airport_trips = df.filter(F.col("is_airport_trip") == True).count()

    logger.info(f"\n[4.12] RESUMEN FASE 4 — TRANSFORMACIÓN:")
    logger.info(f"  Registros entrada    : {initial_count:,}")
    logger.info(f"  Duplicados eliminados: {dups_removed:,}")
    logger.info(f"  Registros salida     : {final_count:,}")
    logger.info(f"  Viajes sospechosos   : {suspicious:,} ({suspicious/final_count*100:.2f}%)")
    logger.info(f"  Viajes aeropuerto    : {airport_trips:,} ({airport_trips/final_count*100:.2f}%)")
    logger.info("═" * 70)

    return df


def save_silver_layer(df: DataFrame, silver_dir: str, config: Dict[str, Any], logger) -> None:
    """
    Persiste el DataFrame transformado en la capa silver.
    Particionado por service_type/year/month para eficiencia en queries.
    """
    logger.info(f"[4.13] Guardando capa silver: {silver_dir}")

    # Reparticionar SOLO para el write (no afecta el paralelismo de los
    # conteos previos, que ya corrieron sobre el DataFrame persistido)
    write_partitions = config["processing"].get("silver_write_partitions", 16)
    df_to_write = df.repartition(write_partitions, "service_type", "year", "month")

    try:
        (
            df_to_write.write
            .mode("overwrite")
            .partitionBy("service_type", "year", "month")
            .parquet(silver_dir)
        )
    except Exception as e:
        logger.error(f"  ❌ Fallo al escribir capa silver: {e}")
        raise

    logger.info(f"  ✅ Capa silver guardada en: {silver_dir}")
    # Nota: se elimina el df.count() posterior — era una acción redundante
    # de 44M filas solo para logging. El conteo final ya se reporta en
    # run_transformation_phase() (paso [4.12]).


def read_silver_layer(spark: SparkSession, silver_dir: str, logger) -> Optional[DataFrame]:
    """Lee la capa silver desde disco (útil para notebooks independientes)."""
    if not Path(silver_dir).exists():
        logger.error(f"Capa silver no encontrada: {silver_dir}")
        return None
    df = spark.read.parquet(silver_dir)
    logger.info(f"Capa silver cargada: {df.count():,} registros")
    return df
