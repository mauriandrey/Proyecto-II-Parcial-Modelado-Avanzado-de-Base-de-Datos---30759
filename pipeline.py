"""
pipeline.py — Orquestador Principal del Pipeline ETL
Proyecto: ETL Spark Parquet Advanced – NYC TLC Trip Records

Ejecuta todas las fases en secuencia:
  Fase 1 → Extracción e inventario de archivos
  Fase 2 → Diagnóstico y reconstrucción de esquema canónico (bronze)
  Fase 3 → Cuarentena de archivos problemáticos
  Fase 4 → Transformación y enriquecimiento (silver)
  Fase 5 → Validación de calidad y separación de rechazados
  Fase 6 → Construcción de tablas analíticas (gold)
  Fase 7 → Carga idempotente en DuckDB + verificación SQL

Características:
  - Idempotente: puede ejecutarse múltiples veces sin duplicar datos
  - Auditable: genera process_id único por ejecución
  - Escalable: configuración externa por YAML
  - Robusto: archivos corruptos no detienen el pipeline
"""

import sys
import os
import json
import argparse
import traceback
from pathlib import Path
from datetime import datetime

# Asegurar que src/ esté en el path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils import (
    generate_process_id, load_config, setup_logger, create_spark_session,
    ensure_dirs, Timer, safe_json_save, get_current_timestamp
)
from src.extract        import run_extraction_phase
from src.schema_recovery import run_schema_recovery_phase, read_bronze_layer
from src.transformations import (
    run_transformation_phase, save_silver_layer, read_silver_layer
)
from src.quality_rules  import run_quality_phase
from src.load           import run_load_phase


# ══════════════════════════════════════════════════════════════════
# ORQUESTADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════

def run_pipeline(
    config_path:   str = "config/etl_config.yaml",
    start_phase:   int = 1,
    end_phase:     int = 7,
    skip_download: bool = False,
) -> dict:
    """
    Ejecuta el pipeline ETL completo o parcial.

    Args:
        config_path:   Ruta al archivo YAML de configuración
        start_phase:   Fase de inicio (1=extracción, puede omitir fases previas)
        end_phase:     Fase de fin (7=carga+verificación)
        skip_download: Si True, omite la descarga (usa archivos ya descargados)

    Returns:
        dict con métricas de ejecución y estado final
    """
    # ── Generar ID de ejecución único ─────────────────────────────
    process_id = generate_process_id()

    # ── Cargar configuración externa ──────────────────────────────
    config = load_config(config_path)

    # ── Configurar logging ────────────────────────────────────────
    log_dir = config["paths"].get("logs_dir", "logs")
    logger  = setup_logger("pipeline", log_dir, process_id)

    logger.info("╔" + "═" * 68 + "╗")
    logger.info("║   ETL SPARK PARQUET ADVANCED — NYC TLC Trip Records           ║")
    logger.info("║   Arquitectura Data Lakehouse — Medallion Pattern              ║")
    logger.info("╚" + "═" * 68 + "╝")
    logger.info(f"  process_id  : {process_id}")
    logger.info(f"  config      : {config_path}")
    logger.info(f"  fases       : {start_phase} → {end_phase}")
    logger.info(f"  timestamp   : {get_current_timestamp()}")
    logger.info(f"  Python      : {sys.version.split()[0]}")

    # ── Crear estructura de directorios ───────────────────────────
    ensure_dirs(config)

    # ── Iniciar SparkSession ──────────────────────────────────────
    logger.info("\nIniciando SparkSession con optimizaciones...")
    spark = create_spark_session(config)
    logger.info(f"  Spark versión: {spark.version}")
    logger.info(f"  Master: {spark.sparkContext.master}")

    # ── Mostrar plan de optimización Spark ────────────────────────
    logger.info("\nOptimizaciones Spark activas:")
    logger.info("  1. mergeSchema=false → esquema explícito, sin inferencia costosa")
    logger.info("  2. filterPushdown=true → predicates empujados al lector Parquet")
    logger.info("  3. AQE=true → coalesce adaptativo de particiones")
    logger.info("  4. maxPartitionBytes → control de tamaño de particiones")
    logger.info("  5. partitionBy() → escritura particionada en todas las capas")

    # ── Variables de estado entre fases ───────────────────────────
    dataframes_by_service = None
    inventory_records     = []
    bronze_df             = None
    silver_df             = None
    gold_tables           = {}
    execution_metrics     = {
        "process_id":    process_id,
        "config_path":   config_path,
        "start_time":    get_current_timestamp(),
        "end_time":      None,
        "phases":        {},
        "status":        "RUNNING",
        "error":         None,
    }

    try:
        # ══════════════════════════════════════════════════════════
        # FASE 1: EXTRACCIÓN E INVENTARIO
        # ══════════════════════════════════════════════════════════
        if start_phase <= 1 <= end_phase:
            with Timer("FASE 1 — Extracción e Inventario", logger) as t:
                if skip_download:
                    logger.info("  [SKIP] Descarga omitida por parámetro --skip-download")
                dataframes_by_service, inventory_records = run_extraction_phase(
                    spark, config, process_id, logger
                )

            execution_metrics["phases"]["fase_1"] = {
                "status":      "OK",
                "elapsed_sec": t.elapsed_seconds(),
                "files_read":  sum(len(v) for v in dataframes_by_service.values()),
                "inventory":   len(inventory_records),
            }

            # Guardar inventario intermedio por si falla una fase posterior
            inv_path = f"data/audit/inventory_checkpoint_{process_id}.json"
            safe_json_save(inventory_records, inv_path)

        # ══════════════════════════════════════════════════════════
        # FASE 2-3: DIAGNÓSTICO, RECONSTRUCCIÓN Y CUARENTENA (BRONZE)
        # ══════════════════════════════════════════════════════════
        if start_phase <= 2 <= end_phase:
            with Timer("FASE 2-3 — Diagnóstico y Reconstrucción (Bronze)", logger) as t:
                if dataframes_by_service is None:
                    # Re-entrada desde fase 2: leer bronze si ya existe
                    logger.info("  Leyendo capa bronze existente...")
                    bronze_df = read_bronze_layer(
                        spark, config["paths"]["bronze_dir"], logger
                    )
                    diagnostics = []
                else:
                    bronze_df, diagnostics = run_schema_recovery_phase(
                        spark, dataframes_by_service, config, process_id, logger
                    )

                # Guardar diagnósticos
                if diagnostics:
                    diag_path = f"data/audit/diagnostics_{process_id}.json"
                    safe_json_save(diagnostics, diag_path)

            if bronze_df is not None:
                execution_metrics["phases"]["fase_2_3"] = {
                    "status":        "OK",
                    "elapsed_sec":   t.elapsed_seconds(),
                    "bronze_records": bronze_df.count(),
                }
            else:
                logger.error("Bronze DataFrame vacío — no se puede continuar")
                raise RuntimeError("Bronze layer empty after schema recovery")

        # ══════════════════════════════════════════════════════════
        # FASE 4: TRANSFORMACIÓN (SILVER)
        # ══════════════════════════════════════════════════════════
        if start_phase <= 4 <= end_phase:
            # Limpiar cache de bronze antes de fase 4
            if bronze_df is not None:
                try:
                    bronze_df.unpersist()
                except Exception:
                    pass
            spark.catalog.clearCache()
            import gc; gc.collect()
            logger.info("[MEM] Cache limpiado antes de Fase 4")
            with Timer("FASE 4 — Transformación (Silver)", logger) as t:
                if bronze_df is None:
                    logger.info("  Leyendo capa bronze desde disco...")
                    bronze_df = read_bronze_layer(
                        spark, config["paths"]["bronze_dir"], logger
                    )
                    if bronze_df is None:
                        raise RuntimeError("No se encontró la capa bronze")

                silver_df = run_transformation_phase(
                    spark, bronze_df, config, process_id, logger
                )

                # Guardar silver
                save_silver_layer(silver_df, config["paths"]["silver_dir"], config, logger)

                # Mostrar plan de ejecución (explain) para documentar optimizaciones
                logger.info("\n  [EXPLAIN] Plan físico de silver (primeras líneas):")
                plan_str = silver_df._jdf.queryExecution().simpleString()
                logger.debug(f"  {plan_str[:500]}...")

            execution_metrics["phases"]["fase_4"] = {
                "status":         "OK",
                "elapsed_sec":    t.elapsed_seconds(),
                "silver_records": silver_df.count(),
            }

        # ══════════════════════════════════════════════════════════
        # FASE 5-6: CALIDAD, VALIDACIÓN Y GOLD
        # ══════════════════════════════════════════════════════════
        if start_phase <= 5 <= end_phase:
            with Timer("FASE 5-6 — Calidad y Gold", logger) as t:
                if silver_df is None:
                    logger.info("  Leyendo capa silver desde disco...")
                    silver_df = read_silver_layer(
                        spark, config["paths"]["silver_dir"], logger
                    )
                    if silver_df is None:
                        raise RuntimeError("No se encontró la capa silver")

                gold_tables = run_quality_phase(
                    spark, silver_df, config, process_id, logger
                )

            execution_metrics["phases"]["fase_5_6"] = {
                "status":       "OK",
                "elapsed_sec":  t.elapsed_seconds(),
                "gold_tables":  list(gold_tables.keys()),
            }

        # ══════════════════════════════════════════════════════════
        # FASE 7: CARGA EN DUCKDB
        # ══════════════════════════════════════════════════════════
        if start_phase <= 7 <= end_phase:
            with Timer("FASE 7 — Carga DuckDB + Verificación SQL", logger) as t:
                verification_results = run_load_phase(
                    spark,
                    gold_tables,
                    inventory_records,
                    config,
                    process_id,
                    logger
                )

            ok_queries = sum(1 for r in verification_results if r["status"] == "OK")
            execution_metrics["phases"]["fase_7"] = {
                "status":          "OK",
                "elapsed_sec":     t.elapsed_seconds(),
                "queries_ok":      ok_queries,
                "queries_total":   len(verification_results),
                "db_path":         config["database"]["path"],
            }

        # ── Éxito ──────────────────────────────────────────────────
        execution_metrics["status"]   = "SUCCESS"
        execution_metrics["end_time"] = get_current_timestamp()

    except Exception as e:
        execution_metrics["status"]   = "FAILED"
        execution_metrics["error"]    = str(e)
        execution_metrics["end_time"] = get_current_timestamp()
        logger.error("\n" + "═" * 70)
        logger.error(f"  PIPELINE FALLIDO: {e}")
        logger.error("═" * 70)
        logger.debug(traceback.format_exc())
        raise

    finally:
        # ── Guardar métricas finales de ejecución ─────────────────
        metrics_path = f"logs/execution_metrics_{process_id}.json"
        safe_json_save(execution_metrics, metrics_path)

        # ── Resumen final ──────────────────────────────────────────
        logger.info("\n" + "╔" + "═" * 68 + "╗")
        logger.info("║   RESUMEN FINAL DEL PIPELINE                                  ║")
        logger.info("╚" + "═" * 68 + "╝")
        logger.info(f"  process_id : {process_id}")
        logger.info(f"  estado     : {execution_metrics['status']}")
        logger.info(f"  inicio     : {execution_metrics['start_time']}")
        logger.info(f"  fin        : {execution_metrics['end_time']}")
        for ph, data in execution_metrics.get("phases", {}).items():
            elapsed = data.get("elapsed_sec", 0)
            status  = data.get("status", "?")
            logger.info(f"  {ph:<15}: {status} ({elapsed:.1f}s)")
        logger.info(f"  métricas   : {metrics_path}")
        if execution_metrics["status"] == "SUCCESS":
            logger.info(f"  base datos : {config['database']['path']}")
        logger.info("╔" + "═" * 68 + "╗\n")

        # Detener Spark al finalizar
        spark.stop()
        logger.info("SparkSession detenida.")

    return execution_metrics


# ══════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA CLI
# ══════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL Spark Parquet Advanced — NYC TLC Trip Records",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Ejecutar pipeline completo
  python pipeline.py

  # Usar configuración personalizada
  python pipeline.py --config config/etl_config.yaml

  # Ejecutar solo fases 4-7 (si bronze ya existe)
  python pipeline.py --start-phase 4 --end-phase 7

  # Saltar descarga (usar archivos ya en disco)
  python pipeline.py --skip-download

  # Solo extracción e inventario
  python pipeline.py --end-phase 1
        """
    )
    parser.add_argument(
        "--config", default="config/etl_config.yaml",
        help="Ruta al archivo de configuración YAML (default: config/etl_config.yaml)"
    )
    parser.add_argument(
        "--start-phase", type=int, default=1, choices=range(1, 8),
        help="Fase de inicio del pipeline (1-7, default: 1)"
    )
    parser.add_argument(
        "--end-phase", type=int, default=7, choices=range(1, 8),
        help="Fase de fin del pipeline (1-7, default: 7)"
    )
    parser.add_argument(
        "--skip-download", action="store_true",
        help="Omitir descarga de archivos (usar caché local)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    metrics = run_pipeline(
        config_path   = args.config,
        start_phase   = args.start_phase,
        end_phase     = args.end_phase,
        skip_download = args.skip_download,
    )
    exit_code = 0 if metrics["status"] == "SUCCESS" else 1
    sys.exit(exit_code)
