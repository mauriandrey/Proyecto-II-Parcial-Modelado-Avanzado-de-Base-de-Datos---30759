"""
extract.py — Fase 1: Extracción, Descarga e Inventario de Archivos Parquet
Proyecto: ETL Spark Parquet Advanced – NYC TLC Trip Records

Responsabilidades:
  - Descargar archivos Parquet desde NYC TLC (CDN oficial) y Apache Parquet Testing
  - Leer cada archivo individualmente, manejando errores sin detener el pipeline
  - Clasificar archivos: SUCCESS / recuperables / no recuperables
  - Construir el inventario técnico (audit_file_inventory)
  - Enviar archivos problemáticos a cuarentena con evidencia técnica
"""

import os
import time
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils import (
    compute_schema_hash, format_size_mb, get_current_timestamp, setup_logger
)


# ══════════════════════════════════════════════════════════════════
# 1. CLASIFICACIÓN DE ERRORES DE LECTURA
# ══════════════════════════════════════════════════════════════════

# Códigos de estado para el inventario de archivos
READ_STATUS = {
    "OK":              "SUCCESS",
    "CORRUPT":         "NOT_RECOVERABLE_CORRUPT_METADATA",
    "EMPTY":           "NOT_RECOVERABLE_EMPTY_FILE",
    "UNSUPPORTED":     "NOT_RECOVERABLE_UNSUPPORTED_FORMAT",
    "SCHEMA_MISMATCH": "RECOVERABLE_SCHEMA_MISMATCH",
    "MISSING_COLS":    "RECOVERABLE_MISSING_COLUMNS",
    "TYPE_CAST":       "RECOVERABLE_TYPE_CASTING",
    "PARTIAL":         "PARTIALLY_RECOVERABLE",
    "UNKNOWN":         "NOT_RECOVERABLE_UNKNOWN",
}


def classify_read_error(error_msg: str) -> str:
    """
    Clasifica el tipo de error de lectura según el mensaje de excepción.
    Esta clasificación determina la acción a tomar sobre el archivo.
    """
    msg = error_msg.lower()
    if any(k in msg for k in ["magic number", "corrupt", "invalid header", "bad magic"]):
        return READ_STATUS["CORRUPT"]
    elif any(k in msg for k in ["no files found", "empty", "file is empty", "0 rows"]):
        return READ_STATUS["EMPTY"]
    elif any(k in msg for k in ["codec", "unsupported", "not supported", "brotli", "lz4"]):
        return READ_STATUS["UNSUPPORTED"]
    elif any(k in msg for k in ["schema", "field", "column name"]):
        return READ_STATUS["SCHEMA_MISMATCH"]
    elif any(k in msg for k in ["missing", "not found", "column"]):
        return READ_STATUS["MISSING_COLS"]
    elif any(k in msg for k in ["type", "cast", "cannot", "convert"]):
        return READ_STATUS["TYPE_CAST"]
    elif "partial" in msg:
        return READ_STATUS["PARTIAL"]
    else:
        return READ_STATUS["UNKNOWN"]


def is_recoverable(status: str) -> bool:
    """Determina si un archivo con este estado puede intentarse recuperar."""
    return status.startswith("RECOVERABLE") or status == "PARTIALLY_RECOVERABLE"


def is_success(status: str) -> bool:
    """Determina si la lectura del archivo fue exitosa."""
    return status == "SUCCESS"


# ══════════════════════════════════════════════════════════════════
# 2. DESCARGA DE ARCHIVOS
# ══════════════════════════════════════════════════════════════════

def download_file(url: str, dest_path: str, logger, timeout: int = 300) -> Tuple[bool, str]:
    """
    Descarga un archivo desde URL con soporte de caché local.
    Si el archivo ya existe, no lo descarga nuevamente (idempotencia).

    Returns:
        (bool: éxito, str: mensaje descriptivo)
    """
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

    if Path(dest_path).exists() and Path(dest_path).stat().st_size > 0:
        size_mb = format_size_mb(Path(dest_path).stat().st_size)
        logger.info(f"    [CACHE] {Path(dest_path).name} ({size_mb} MB) — ya existe, omitiendo descarga")
        return True, "cached"

    logger.info(f"    [DOWN]  Descargando: {url}")
    try:
        resp = requests.get(url, stream=True, timeout=timeout,
                            headers={"User-Agent": "ETL-Pipeline/1.0"})
        resp.raise_for_status()

        bytes_written = 0
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                f.write(chunk)
                bytes_written += len(chunk)

        size_mb = format_size_mb(bytes_written)
        logger.info(f"    [OK]    {Path(dest_path).name} ({size_mb} MB)")
        return True, "downloaded"

    except requests.HTTPError as e:
        logger.error(f"    [HTTP]  Error HTTP {e.response.status_code}: {url}")
        _cleanup_partial(dest_path)
        return False, f"HTTP {e.response.status_code}"
    except requests.ConnectionError as e:
        logger.error(f"    [CONN]  Sin conexión para: {url}")
        _cleanup_partial(dest_path)
        return False, "connection_error"
    except Exception as e:
        logger.error(f"    [ERR]   {url}: {e}")
        _cleanup_partial(dest_path)
        return False, str(e)[:200]


def _cleanup_partial(path: str) -> None:
    """Elimina archivos parcialmente descargados."""
    if Path(path).exists():
        Path(path).unlink(missing_ok=True)


def download_all_sources(config: Dict[str, Any], logger) -> Dict[str, int]:
    """
    Descarga todos los archivos configurados: NYC TLC + Apache Parquet Testing (bad files).
    Retorna conteo de éxitos por fuente.
    """
    raw_dir = config["paths"]["raw_dir"]
    sources = config.get("data_sources", {})
    bad_sources = config.get("bad_parquet_sources", {})
    stats = {"downloaded": 0, "cached": 0, "failed": 0}

    logger.info("── Descargando archivos NYC TLC ───────────────────────────────")
    for service, src_cfg in sources.items():
        base_url = src_cfg["base_url"]
        logger.info(f"  Servicio: {service.upper()}")
        for file_info in src_cfg["files"]:
            fname = file_info["name"]
            year  = file_info["year"]
            month = file_info["month"]
            dest_dir  = os.path.join(raw_dir, service, f"year={year}", f"month={month}")
            dest_path = os.path.join(dest_dir, fname)
            url = f"{base_url}/{fname}"

            ok, reason = download_file(url, dest_path, logger)
            if ok:
                stats["downloaded" if reason == "downloaded" else "cached"] += 1
            else:
                stats["failed"] += 1

    logger.info("\n── Descargando archivos Apache Parquet Testing (bad files) ────")
    bad_url = bad_sources.get("base_url", "")
    bad_dir = os.path.join(raw_dir, "bad_parquet")
    for fname in bad_sources.get("files", []):
        url = f"{bad_url}/{fname}"
        dest_path = os.path.join(bad_dir, fname)
        ok, reason = download_file(url, dest_path, logger)
        if ok:
            stats["downloaded" if reason == "downloaded" else "cached"] += 1
        else:
            stats["failed"] += 1

    logger.info(f"\n  Resumen: {stats['downloaded']} nuevos | {stats['cached']} en caché | {stats['failed']} fallidos")
    return stats


# ══════════════════════════════════════════════════════════════════
# 3. DETECCIÓN DE TIPO DE SERVICIO
# ══════════════════════════════════════════════════════════════════

def detect_service_type(file_path: str) -> str:
    """Infiere el tipo de servicio desde el nombre del archivo o la ruta."""
    name  = Path(file_path).name.lower()
    route = str(file_path).lower()

    if "yellow" in name or "/yellow/" in route:
        return "yellow"
    elif "green" in name or "/green/" in route:
        return "green"
    elif "fhvhv" in name or "/fhvhv/" in route:
        return "fhvhv"
    elif "bad_parquet" in route or "bad" in route:
        return "bad_parquet"
    else:
        return "unknown"


def extract_partition_value(file_path: str, key: str) -> Optional[str]:
    """
    Extrae el valor de una partición Hive-style del path (ej: year=2023/month=01).
    Si no está en el path, intenta inferirlo del nombre del archivo.
    """
    # Intentar desde path particionado
    for part in Path(file_path).parts:
        if part.startswith(f"{key}="):
            return part.split("=", 1)[1]

    # Fallback: inferir del nombre del archivo (ej: yellow_tripdata_2023-01.parquet)
    stem = Path(file_path).stem  # e.g. "yellow_tripdata_2023-01"
    tokens = stem.split("-")
    if key == "year" and len(tokens) >= 2:
        candidate = tokens[-2]
        if len(candidate) == 4 and candidate.isdigit():
            return candidate
    if key == "month" and len(tokens) >= 1:
        candidate = tokens[-1]
        if len(candidate) == 2 and candidate.isdigit():
            return candidate

    return None


# ══════════════════════════════════════════════════════════════════
# 4. LECTURA INDIVIDUAL DE ARCHIVOS PARQUET
# ══════════════════════════════════════════════════════════════════

def try_read_parquet(
    spark:        SparkSession,
    file_path:    str,
    service_type: str,
    process_id:   str,
    logger
) -> Tuple[Optional[DataFrame], Dict[str, Any]]:
    """
    Intenta leer un archivo Parquet de forma segura.

    Estrategia:
      - Usa modo PERMISSIVE para tolerar registros mal formados
      - Captura excepciones para no detener el pipeline
      - Clasifica el error para determinar si el archivo es recuperable

    Returns:
        (DataFrame o None, dict con registro de inventario)
    """
    fname = Path(file_path).name
    stat  = Path(file_path).stat() if Path(file_path).exists() else None

    # Registro inicial del inventario
    inv = {
        "process_id":    process_id,
        "source_system": "NYC_TLC" if service_type != "bad_parquet" else "Apache_Parquet_Testing",
        "service_type":  service_type,
        "file_name":     fname,
        "file_path":     str(file_path),
        "file_size_mb":  format_size_mb(stat.st_size) if stat else 0.0,
        "partition_year":  extract_partition_value(file_path, "year"),
        "partition_month": extract_partition_value(file_path, "month"),
        "read_status":   "PENDING",
        "record_count":  0,
        "column_count":  0,
        "schema_hash":   None,
        "error_message": None,
        "processed_at":  get_current_timestamp(),
    }

    if not Path(file_path).exists():
        inv["read_status"] = "NOT_RECOVERABLE_FILE_NOT_FOUND"
        inv["error_message"] = "Archivo no encontrado en disco"
        return None, inv

    if stat and stat.st_size == 0:
        inv["read_status"] = READ_STATUS["EMPTY"]
        inv["error_message"] = "Archivo vacío (0 bytes)"
        return None, inv

    try:
        # Lectura con esquema permisivo — Optimización: mergeSchema=false
        df = (
            spark.read
            .option("mergeSchema",              "false")
            .option("mode",                     "PERMISSIVE")
            .option("datetimeRebaseMode",       "CORRECTED")
            .option("int96RebaseMode",          "CORRECTED")
            .parquet(file_path)
        )

        # Materializar para detectar errores en lectura real (lazy evaluation)
        record_count = df.count()
        col_count    = len(df.columns)
        schema_hash  = compute_schema_hash(df.schema)

        inv.update({
            "read_status":  READ_STATUS["OK"],
            "record_count": record_count,
            "column_count": col_count,
            "schema_hash":  schema_hash,
        })

        logger.info(f"    [OK]  {fname:50s} | {record_count:>10,} registros | {col_count} cols")
        return df, inv

    except Exception as e:
        err_msg  = str(e)
        err_type = classify_read_error(err_msg)

        inv.update({
            "read_status":   err_type,
            "error_message": err_msg[:500],
        })

        # Distinguir claramente entre recuperable y no recuperable
        tag = "⚠ RECUP" if is_recoverable(err_type) else "✗ FATAL"
        logger.warning(f"    [{tag}] {fname:50s} | {err_type}")
        logger.debug(f"             Detalle: {err_msg[:200]}")

        return None, inv


# ══════════════════════════════════════════════════════════════════
# 5. CUARENTENA DE ARCHIVOS
# ══════════════════════════════════════════════════════════════════

def quarantine_file_record(
    inv_record:    Dict[str, Any],
    quarantine_dir: str,
    logger
) -> None:
    """
    Registra un archivo fallido en la cuarentena con toda la evidencia técnica.
    No mueve el archivo original (se conserva en raw), solo registra el rechazo.
    """
    import json
    qdir = Path(quarantine_dir) / "files"
    qdir.mkdir(parents=True, exist_ok=True)

    fname    = inv_record["file_name"].replace(".parquet", "")
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = qdir / f"rejected_{fname}_{ts}.json"

    quarantine_entry = {
        **inv_record,
        "quarantine_timestamp":  get_current_timestamp(),
        "quarantine_stage":      "EXTRACTION",
        "recommended_action":    _recommend_action(inv_record["read_status"]),
        "is_recoverable":        is_recoverable(inv_record["read_status"]),
    }

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(quarantine_entry, f, ensure_ascii=False, indent=2, default=str)

    logger.debug(f"    [QUAR] Registrado en cuarentena: {out_file.name}")


def _recommend_action(status: str) -> str:
    """Genera una recomendación de acción basada en el estado del archivo."""
    actions = {
        "SUCCESS":                          "Ninguna — archivo válido",
        "NOT_RECOVERABLE_CORRUPT_METADATA": "Solicitar re-extracción del archivo fuente. Verificar integridad con parquet-tools.",
        "NOT_RECOVERABLE_EMPTY_FILE":       "Verificar en fuente si el mes tiene datos. Podría ser un mes sin viajes.",
        "NOT_RECOVERABLE_UNSUPPORTED_FORMAT":"Actualizar versión de PySpark o verificar codec disponible.",
        "RECOVERABLE_SCHEMA_MISMATCH":      "Aplicar reconstrucción de esquema canónico en Fase 2.",
        "RECOVERABLE_MISSING_COLUMNS":      "Completar columnas faltantes con NULL en Fase 2.",
        "RECOVERABLE_TYPE_CASTING":         "Aplicar conversión de tipos controlada en Fase 2.",
        "PARTIALLY_RECOVERABLE":            "Intentar lectura parcial y enviar registros inválidos a cuarentena.",
        "NOT_RECOVERABLE_UNKNOWN":          "Analizar el error manualmente. Considerar re-descarga.",
    }
    return actions.get(status, "Analizar manualmente.")


# ══════════════════════════════════════════════════════════════════
# 6. FASE DE EXTRACCIÓN COMPLETA
# ══════════════════════════════════════════════════════════════════

def collect_parquet_files(raw_dir: str) -> List[Tuple[str, str]]:
    """Recolecta todos los archivos .parquet del directorio raw con su tipo de servicio."""
    files = []
    for root, _, filenames in os.walk(raw_dir):
        for fname in sorted(filenames):
            if fname.endswith(".parquet"):
                full_path = os.path.join(root, fname)
                service   = detect_service_type(full_path)
                files.append((full_path, service))
    return sorted(files)


def run_extraction_phase(
    spark:      SparkSession,
    config:     Dict[str, Any],
    process_id: str,
    logger
) -> Tuple[Dict[str, List[Tuple[str, DataFrame]]], List[Dict[str, Any]]]:
    """
    Ejecuta la Fase 1 completa del pipeline ETL.

    Flujo:
      1. Descarga archivos (con caché para idempotencia)
      2. Lee cada archivo Parquet individualmente, capturando errores
      3. Registra el inventario técnico para cada archivo
      4. Envía archivos problemáticos a cuarentena con evidencia
      5. Retorna DataFrames válidos agrupados por tipo de servicio

    Returns:
        dataframes_by_service: dict {service_type: [(path, DataFrame), ...]}
        inventory_records:     lista de registros para audit_file_inventory
    """
    logger.info("═" * 70)
    logger.info(f"  FASE 1: EXTRACCIÓN  —  process_id: {process_id}")
    logger.info("═" * 70)

    raw_dir       = config["paths"]["raw_dir"]
    quarantine_dir = config["paths"]["quarantine_dir"]

    # ── 1. Descargar archivos ──────────────────────────────────────
    logger.info("\n[1.1] Descarga de archivos fuente:")
    download_all_sources(config, logger)

    # ── 2. Recolectar archivos en disco ───────────────────────────
    all_files = collect_parquet_files(raw_dir)
    logger.info(f"\n[1.2] Total de archivos .parquet encontrados: {len(all_files)}")

    # ── 3. Leer cada archivo individualmente ──────────────────────
    logger.info("\n[1.3] Lectura individual de archivos:")
    inventory_records   = []
    dataframes_by_service = {"yellow": [], "green": [], "fhvhv": [], "bad_parquet": []}

    for file_path, service_type in all_files:
        df, inv_rec = try_read_parquet(spark, file_path, service_type, process_id, logger)
        inventory_records.append(inv_rec)

        if df is not None and service_type in ("yellow", "green", "fhvhv"):
            # Agregar columna fuente para trazabilidad
            df = df.withColumn("_source_file", F.lit(Path(file_path).name))
            dataframes_by_service[service_type].append((file_path, df))
        elif not is_success(inv_rec["read_status"]):
            quarantine_file_record(inv_rec, quarantine_dir, logger)

    # ── 4. Resumen ─────────────────────────────────────────────────
    total     = len(inventory_records)
    ok_count  = sum(1 for r in inventory_records if is_success(r["read_status"]))
    err_count = total - ok_count
    total_rec = sum(r["record_count"] for r in inventory_records)

    logger.info("\n[1.4] RESUMEN FASE 1 — EXTRACCIÓN:")
    logger.info(f"  Archivos procesados : {total}")
    logger.info(f"  Lecturas exitosas   : {ok_count}")
    logger.info(f"  Archivos fallidos   : {err_count}")
    logger.info(f"  Registros totales   : {total_rec:,}")
    logger.info(f"  DataFrames yellow   : {len(dataframes_by_service['yellow'])}")
    logger.info(f"  DataFrames green    : {len(dataframes_by_service['green'])}")
    logger.info(f"  DataFrames fhvhv    : {len(dataframes_by_service['fhvhv'])}")
    logger.info("═" * 70)

    return dataframes_by_service, inventory_records
