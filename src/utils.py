
# ── Parche automático Windows (generado por setup_windows.py) ──
import platform as _platform
if _platform.system() == "Windows":
    import os as _os
    _hadoop = r"C:\Users\mauri\hadoop"
    _os.environ.setdefault("HADOOP_HOME",     _hadoop)
    _os.environ.setdefault("hadoop.home.dir", _hadoop)
    _os.environ["PATH"] = _os.path.join(_hadoop, "bin") + ";" + _os.environ.get("PATH", "")
# ─────────────────────────────────────────────────────────────
"""
utils.py — Utilidades comunes del Pipeline ETL
Proyecto: ETL Spark Parquet Advanced – NYC TLC Trip Records
Arquitectura: Data Lakehouse Medallion (raw → bronze → silver → gold)
"""
# ── Configuración automática Windows (HADOOP_HOME) ────────────────
import platform as _platform
if _platform.system() == "Windows":
    import os as _os
    from pathlib import Path as _Path
    _hadoop_candidates = [
        _os.environ.get("HADOOP_HOME", ""),
        str(_Path.home() / "hadoop"),
        r"C:\hadoop",
        r"C:\tools\hadoop",
    ]
    _hadoop_home = next(
        (p for p in _hadoop_candidates if p and (_Path(p) / "bin" / "winutils.exe").exists()),
        str(_Path.home() / "hadoop")
    )
    _os.environ.setdefault("HADOOP_HOME",     _hadoop_home)
    _os.environ.setdefault("hadoop.home.dir", _hadoop_home)
    _bin = str(_Path(_hadoop_home) / "bin")
    if _bin not in _os.environ.get("PATH", ""):
        _os.environ["PATH"] = _bin + ";" + _os.environ.get("PATH", "")
    _tmp = str(_Path.home() / "spark_tmp")
    _Path(_tmp).mkdir(parents=True, exist_ok=True)
    _os.environ.setdefault("SPARK_LOCAL_DIRS", _tmp)
    _os.environ.setdefault("TMPDIR",           _tmp)
# ─────────────────────────────────────────────────────────────────
import uuid
import hashlib
import logging
import os
import sys
import json
import time
import yaml
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


# ══════════════════════════════════════════════════════════════════
# 1. IDENTIFICADOR DE EJECUCIÓN
# ══════════════════════════════════════════════════════════════════

def generate_process_id() -> str:
    """
    Genera un identificador único e irrepetible por ejecución del pipeline.
    Formato: ETL-YYYYMMDD-HHMMSS-XXXXXXXX
    Garantiza idempotencia: dos ejecuciones nunca comparten el mismo process_id.
    """
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    uid = str(uuid.uuid4())[:8].upper()
    return f"ETL-{ts}-{uid}"


# ══════════════════════════════════════════════════════════════════
# 2. CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════

def load_config(config_path: str = "config/etl_config.yaml") -> Dict[str, Any]:
    """Carga la configuración desde el archivo YAML externo."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg


def load_metadata(metadata_path: str) -> Dict[str, Any]:
    """Carga un archivo de metadatos JSON."""
    with open(metadata_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════════
# 3. LOGGING ESTRUCTURADO
# ══════════════════════════════════════════════════════════════════

def setup_logger(name: str, log_dir: str = "logs", process_id: str = "default") -> logging.Logger:
    """
    Configura un logger con salida a consola y a archivo de log.
    Cada ejecución genera su propio archivo de log identificado por process_id.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Limpiar handlers anteriores para evitar duplicados
    if logger.handlers:
        logger.handlers.clear()

    fmt = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    # Handler: consola (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Handler: archivo (DEBUG+)
    log_file = os.path.join(log_dir, f"pipeline_{process_id}.log")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


# ══════════════════════════════════════════════════════════════════
# 4. SESIÓN SPARK OPTIMIZADA
# ══════════════════════════════════════════════════════════════════

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Crea la SparkSession con configuraciones de optimización:

    Optimización 1: Esquema explícito (mergeSchema=false) — evita inferSchema costoso
    Optimización 2: Predicate pushdown habilitado — reduce datos leídos del disco
    Optimización 3: Adaptive Query Execution (AQE) — optimiza joins y coalesce
    Optimización 4: Tamaño de partición controlado — manejo de archivos pequeños
    Optimización 5: Escritura particionada — mejor rendimiento en consultas
    """
    sc = config.get("spark", {})

    builder = (
        SparkSession.builder
        .appName(sc.get("app_name", "ETL_NYC_TLC"))
        .master(sc.get("master", "local[4]"))
        # ── Memoria (optimizada para 32GB Windows) ─────────────────
        .config("spark.driver.memory",        sc.get("driver_memory", "16g"))
        .config("spark.executor.memory",      sc.get("executor_memory", "16g"))
        .config("spark.driver.maxResultSize", sc.get("driver_max_result_size", "2g"))
        # Memoria fuera de heap para reducir GC pressure
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size",    "2g")
        # ── Opt 1: Esquema explícito ───────────────────────────────
        .config("spark.sql.parquet.mergeSchema",              "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.timeParserPolicy",          "LEGACY")
        # ── Opt 2: Predicate pushdown ──────────────────────────────
        .config("spark.sql.parquet.filterPushdown",           "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        # ── Opt 3: AQE adaptativo ──────────────────────────────────
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled",           "true")
        # ── Opt 4: Particiones reducidas (anti-OOM) ────────────────
        .config("spark.sql.shuffle.partitions",
                str(sc.get("shuffle_partitions", 50)))
        .config("spark.sql.files.maxPartitionBytes",
                str(sc.get("max_partition_bytes_mb", 64) * 1024 * 1024))
        .config("spark.sql.files.openCostInBytes",
                str(sc.get("open_cost_bytes_mb", 4) * 1024 * 1024))
        # ── Opt 5: Escritura controlada ────────────────────────────
        .config("spark.sql.files.maxRecordsPerFile", "2000000")
        # GC agresivo para liberar memoria en Windows
        .config("spark.driver.extraJavaOptions",
                "-XX:+UseG1GC -XX:G1HeapRegionSize=16m "
                "-XX:+PrintGCDetails -XX:InitiatingHeapOccupancyPercent=35")
        # Serialización eficiente
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        # UI
        .config("spark.ui.showConsoleProgress", "false")
    )

    # ── Ajustes específicos Windows ───────────────────────────────
    import platform as _plt
    if _plt.system() == "Windows":
        from pathlib import Path as _P
        tmp_dir = str(_P.home() / "spark_tmp")
        _P(tmp_dir).mkdir(parents=True, exist_ok=True)
        builder = (
            builder
            .config("spark.local.dir", tmp_dir)
            .config("spark.driver.extraJavaOptions",
                    f"-Djava.io.tmpdir={tmp_dir}")
            .config("spark.sql.shuffle.partitions", "50")
            .config("spark.sql.files.maxPartitionBytes", "67108864")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(sc.get("log_level", "WARN"))
    return spark


# ══════════════════════════════════════════════════════════════════
# 5. FUNCIONES DE HASHING
# ══════════════════════════════════════════════════════════════════

def compute_schema_hash(schema: StructType) -> str:
    """
    Genera un hash MD5 del esquema Spark para detectar cambios estructurales.
    Permite comparar esquemas entre ejecuciones y entre archivos.
    """
    schema_str = json.dumps(schema.jsonValue(), sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()


def compute_file_hash(file_path: str) -> str:
    """Calcula hash MD5 del contenido del archivo para verificar integridad."""
    hasher = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return "UNREADABLE"


# ══════════════════════════════════════════════════════════════════
# 6. GESTIÓN DE DIRECTORIOS
# ══════════════════════════════════════════════════════════════════

def ensure_dirs(config: Dict[str, Any]) -> None:
    """Crea toda la estructura de directorios del proyecto si no existe."""
    dirs_to_create = [
        config["paths"].get("bronze_dir",     "data/bronze"),
        config["paths"].get("silver_dir",     "data/silver"),
        config["paths"].get("gold_dir",       "data/gold"),
        config["paths"].get("quarantine_dir", "data/quarantine"),
        config["paths"].get("audit_dir",      "data/audit"),
        config["paths"].get("logs_dir",       "logs"),
        config["paths"].get("warehouse_dir",  "data/warehouse"),
        "data/raw/yellow",
        "data/raw/green",
        "data/raw/fhvhv",
        "data/raw/bad_parquet",
        "data/quarantine/files",
        "data/quarantine/records",
        "data/gold/trips",
        "data/gold/daily",
        "data/gold/location",
    ]
    for d in dirs_to_create:
        Path(d).mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════
# 7. UTILIDADES GENERALES
# ══════════════════════════════════════════════════════════════════

def get_current_timestamp() -> str:
    """Retorna el timestamp actual en formato ISO 8601."""
    return datetime.now().isoformat()


def safe_json_save(obj: Any, path: str) -> None:
    """Guarda un objeto Python como JSON con formato legible."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


def format_number(n: int) -> str:
    """Formatea un número con separadores de miles para legibilidad."""
    return f"{n:,}"


def format_size_mb(size_bytes: int) -> float:
    """Convierte bytes a megabytes redondeados a 2 decimales."""
    return round(size_bytes / (1024 * 1024), 2)


class Timer:
    """Contexto para medir tiempo de ejecución de fases del pipeline."""

    def __init__(self, phase_name: str, logger):
        self.phase_name = phase_name
        self.logger = logger
        self.start = None

    def __enter__(self):
        self.start = time.perf_counter()
        self.logger.info(f"▶ Iniciando: {self.phase_name}")
        return self

    def __exit__(self, *args):
        elapsed = time.perf_counter() - self.start
        mins, secs = divmod(elapsed, 60)
        self.logger.info(f"✓ Completado: {self.phase_name} — {int(mins)}m {secs:.1f}s")

    def elapsed_seconds(self) -> float:
        return time.perf_counter() - self.start if self.start else 0.0
