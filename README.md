# ETL Spark Parquet Advanced — NYC TLC Trip Records
## Proyecto II Parcial — Modelado Avanzado de Base de Datos

Pipeline ETL avanzado con Apache Spark para procesar, limpiar y cargar datos históricos
de viajes de taxi en Nueva York, implementando una arquitectura **Data Lakehouse Medallion**.

---

---

## ⚠️ Configuración obligatoria en Windows (leer primero)

PySpark en Windows requiere `winutils.exe` y la variable `HADOOP_HOME` configurada.
**Esto se resuelve en un solo paso:**

### Paso único — ejecutar el script de setup
```cmd
python setup_windows.py
```

Este script automáticamente:
- Detecta si Java está instalado
- Descarga `winutils.exe` y `hadoop.dll` (Hadoop 3.3.6)
- Configura `HADOOP_HOME` de forma permanente
- Verifica todas las dependencias Python
- Parchea `src/utils.py` para detectar Windows en cada ejecución

### Si el download falla (sin internet):
1. Descargar manualmente desde: https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin
2. Archivos: `winutils.exe` y `hadoop.dll`
3. Colocar en: `C:\Users\TU_USUARIO\hadoop\bin\`
4. Agregar en Variables de Entorno del Sistema:
   - `HADOOP_HOME` = `C:\Users\TU_USUARIO\hadoop`
5. Reiniciar terminal/IDE y ejecutar el pipeline

### Verificar que funciona
```cmd
# En CMD (no PowerShell)
echo %HADOOP_HOME%
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Arquitectura implementada

```
Fuentes de datos (NYC TLC + Apache Parquet Testing)
         │
         ▼
  ┌─────────────┐
  │   data/raw  │  ← Archivos originales sin modificar
  └─────────────┘
         │ Fase 1: Extracción e inventario
         ▼
  ┌──────────────┐
  │ data/bronze  │  ← Datos leídos + esquema canónico unificado
  └──────────────┘
         │ Fase 2-3: Diagnóstico y reconstrucción
         ▼
  ┌──────────────┐
  │ data/silver  │  ← Datos transformados + métricas derivadas
  └──────────────┘
         │ Fase 4-5: Transformación + calidad
         ▼
  ┌─────────────┐   ┌────────────────┐   ┌───────────────┐
  │  data/gold  │   │ data/quarantine│   │  data/audit   │
  └─────────────┘   └────────────────┘   └───────────────┘
         │ Fase 6-7: Gold + carga DuckDB
         ▼
  ┌──────────────────────────────┐
  │  data/warehouse/nyc_tlc.duckdb │  ← Base de datos consultable
  └──────────────────────────────┘
```

---

## Estructura del proyecto

```
etl_spark_parquet_advanced/
├── pipeline.py                    # Orquestador principal
├── requirements.txt               # Dependencias Python
├── README.md
│
├── config/
│   └── etl_config.yaml            # Configuración externa (fuentes, paths, reglas)
│
├── src/
│   ├── utils.py                   # Utilidades: logger, Spark, config, hashing
│   ├── extract.py                 # Fase 1: Descarga, lectura e inventario
│   ├── schema_recovery.py         # Fase 2-3: Diagnóstico y homologación
│   ├── transformations.py         # Fase 4: Métricas derivadas y deduplicación
│   ├── quality_rules.py           # Fase 5-6: Calidad + tablas Gold
│   └── load.py                    # Fase 7: Carga DuckDB + SQL
│
├── notebooks/
│   ├── 01_extraccion.ipynb
│   ├── 02_diagnostico_reconstruccion.ipynb
│   ├── 03_transformacion_validacion.ipynb
│   ├── 04_carga_base_datos.ipynb
│   └── 05_reporte_calidad_conclusiones.ipynb
│
├── metadata/
│   ├── expected_schema_yellow.json
│   ├── expected_schema_green.json
│   ├── expected_schema_fhvhv.json
│   ├── canonical_schema_trips.json
│   └── business_rules.json
│
└── data/
    ├── raw/           # Archivos originales (nunca modificados)
    ├── bronze/        # Datos unificados bajo esquema canónico
    ├── silver/        # Datos transformados y enriquecidos
    ├── gold/          # Tablas analíticas finales
    ├── quarantine/    # Archivos y registros rechazados
    ├── audit/         # Métricas, inventario y reportes
    └── warehouse/     # nyc_tlc.duckdb (base de datos final)
```

---

## Instalación del entorno

### Requisitos previos
- Python >= 3.9
- Java 11 o 17 (requerido por PySpark)
- 8 GB RAM mínimo (16 GB recomendado)
- 10 GB espacio en disco

### 1. Clonar/descomprimir el proyecto
```bash
cd etl_spark_parquet_advanced
```

### 2. Instalar Java (si no está instalado)
```bash
# Ubuntu/Debian
sudo apt-get install -y openjdk-11-jdk
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# MacOS
brew install openjdk@11

# Verificar
java -version
```

### 3. Crear entorno virtual e instalar dependencias
```bash
python -m venv .venv
source .venv/bin/activate          # Linux/Mac
# .venv\Scripts\activate           # Windows

pip install --upgrade pip
pip install -r requirements.txt
```

---

## Configuración

El pipeline se controla completamente desde `config/etl_config.yaml`.

### Parámetros principales
```yaml
spark:
  master: "local[*]"          # Cambiar a cluster URL si es necesario
  executor_memory: "4g"       # Ajustar según RAM disponible
  driver_memory: "4g"

database:
  path: "data/warehouse/nyc_tlc.duckdb"   # Ruta de la base de datos

quality_rules:
  max_trip_duration_minutes: 480
  max_speed_mph: 100
  max_tip_percentage: 100
```

---

## Ejecución del pipeline

### Opción A — Pipeline completo (recomendado)
```bash
python pipeline.py
```

### Opción B — Con configuración personalizada
```bash
python pipeline.py --config config/etl_config.yaml
```

### Opción C — Saltando la descarga (archivos ya en disco)
```bash
python pipeline.py --skip-download
```

### Opción D — Rango de fases específico
```bash
# Solo extracción e inventario (Fase 1)
python pipeline.py --end-phase 1

# Transformación en adelante (bronze ya existe)
python pipeline.py --start-phase 4 --end-phase 7

# Solo carga en base de datos (silver/gold ya existen)
python pipeline.py --start-phase 7
```

### Opción E — Notebooks interactivos
```bash
cd notebooks
jupyter notebook
# Ejecutar en orden: 01, 02, 03, 04, 05
```

---

## Descarga de datos

### Automática (al ejecutar el pipeline)
El pipeline descarga automáticamente todos los archivos configurados en `etl_config.yaml`.

### Manual (opcional)
```bash
# NYC TLC Yellow Taxi 2023
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet \
     -P data/raw/yellow/year=2023/month=01/

# Apache Parquet Testing (archivos problemáticos)
wget https://github.com/apache/parquet-testing/raw/master/data/PARQUET-1481.parquet \
     -P data/raw/bad_parquet/
```

---

## Base de datos utilizada

**DuckDB** — Base de datos analítica columnar embebida.

### Ventajas para este proyecto
- Sin servidor: no requiere instalación ni configuración de servidor
- SQL completo y compatible con Pandas/Arrow/Parquet
- Rendimiento OLAP excepcional para datasets medianos
- Idempotencia garantizada por `process_id` único por ejecución

### Acceso directo a la base de datos
```python
import duckdb

con = duckdb.connect('data/warehouse/nyc_tlc.duckdb')

# Consultar viajes
con.execute("SELECT service_type, COUNT(*) FROM gold_trips_clean GROUP BY 1").df()

# Ver inventario
con.execute("SELECT * FROM audit_file_inventory").df()
```

---

## Validación de resultados

### Consultas SQL de verificación
```sql
-- 1. Total de viajes e ingresos por servicio
SELECT service_type, COUNT(*) AS total_trips,
       SUM(total_amount) AS total_revenue
FROM gold_trips_clean
GROUP BY service_type ORDER BY total_revenue DESC;

-- 2. Métricas de calidad por período
SELECT service_type, year, month, quality_percentage
FROM quality_metrics_summary
ORDER BY year, month;

-- 3. Top 20 rutas
SELECT pickup_location_id, dropoff_location_id,
       COUNT(*) AS trips, SUM(total_amount) AS revenue
FROM gold_trips_clean
GROUP BY 1, 2 ORDER BY revenue DESC LIMIT 20;
```

---

## Optimizaciones Spark implementadas

| # | Optimización | Implementación |
|---|---|---|
| 1 | Esquema explícito | `mergeSchema=false`, sin `inferSchema` |
| 2 | Predicate pushdown | `parquet.filterPushdown=true` |
| 3 | Partition pruning | Escritura particionada + filtros en partición |
| 4 | AQE (Adaptive) | `spark.sql.adaptive.enabled=true` |
| 5 | Manejo de archivos pequeños | `coalesce()` + `maxPartitionBytes` |
| 6 | Escritura particionada | `partitionBy("service_type","year","month")` |
| 7 | Column pruning | `select()` solo columnas necesarias |
| 8 | Plan de ejecución | `explain(mode='formatted')` en notebooks |

---

## Características del pipeline

### Idempotencia
- Cada ejecución genera un `process_id` único (`ETL-YYYYMMDD-HHMMSS-XXXXXXXX`)
- En DuckDB: `DELETE WHERE process_id = ?` antes de insertar (sin duplicados)
- Descarga con caché: no re-descarga archivos ya existentes

### Trazabilidad
- Todos los archivos quedan registrados en `audit_file_inventory`
- Cada registro rechazado tiene causa técnica y de negocio documentada
- Los archivos en cuarentena incluyen la excepción original completa

### Escalabilidad
- Configuración externa YAML (sin cambios en código para ajustes)
- Spark con modo `local[*]` (usa todos los cores) o configurable a cluster
- Escritura particionada para consultas eficientes en grandes volúmenes

---

## Tablas generadas

| Tabla | Capa | Descripción |
|---|---|---|
| `gold_trips_clean` | Gold | Viajes limpios y validados |
| `gold_daily_revenue` | Gold | Ingresos diarios por servicio |
| `gold_location_performance` | Gold | Rendimiento por zonas |
| `quality_rejected_records` | Quarantine | Registros rechazados con causa |
| `quality_metrics_summary` | Audit | Métricas de calidad por período |
| `audit_file_inventory` | Audit | Inventario técnico de archivos |

---

## Solución de problemas comunes

### Error: Java no encontrado
```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH=$JAVA_HOME/bin:$PATH
```

### Error: OutOfMemoryError en Spark
Reducir datos de prueba o aumentar memoria en `config/etl_config.yaml`:
```yaml
spark:
  executor_memory: "8g"
  driver_memory: "8g"
```

### Bronze/Silver vacíos
Verificar que los archivos fueron descargados en `data/raw/` antes de ejecutar fases 2+.

### DuckDB: tabla ya existe
El pipeline es idempotente — puede re-ejecutarse sin problemas. Si hay error de esquema,
eliminar el archivo `.duckdb` y re-ejecutar desde fase 7.
