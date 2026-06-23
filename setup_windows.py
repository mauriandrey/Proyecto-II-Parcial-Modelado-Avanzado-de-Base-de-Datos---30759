"""
setup_windows.py — Configuración automática de PySpark en Windows
Ejecutar UNA SOLA VEZ antes del pipeline, como administrador.
Soluciona: HADOOP_HOME and hadoop.home.dir are unset (WindowsProblems)
"""
import os, sys, shutil, subprocess, platform, ctypes, urllib.request
from pathlib import Path

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def check_java():
    print("\n[1/4] Verificando Java...")
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        version_line = result.stderr or result.stdout
        print(f"  ✓ Java encontrado: {version_line.splitlines()[0]}")
        if "17" in version_line or "11" in version_line:
            print("  ✓ Versión compatible (11 o 17)")
        else:
            print("  ⚠ Se recomienda Java 11 o 17")
            print("    Descargar en: https://adoptium.net/")
        return True
    except FileNotFoundError:
        print("  ✗ Java NO encontrado.")
        print("  ► Instalar Java 17 desde: https://adoptium.net/")
        print("    y agregar al PATH antes de continuar.")
        return False

def setup_hadoop_home():
    print("\n[2/4] Configurando HADOOP_HOME para Windows...")

    # Directorio local donde pondremos winutils
    hadoop_home = Path(os.path.expanduser("~")) / "hadoop"
    bin_dir     = hadoop_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    winutils_path = bin_dir / "winutils.exe"
    hadoop_dll    = bin_dir / "hadoop.dll"

    # URLs de winutils para Hadoop 3.3.6 (compatible con PySpark 3.5.x)
    BASE_URL = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin"
    files_to_download = {
        "winutils.exe": f"{BASE_URL}/winutils.exe",
        "hadoop.dll":   f"{BASE_URL}/hadoop.dll",
    }

    for fname, url in files_to_download.items():
        dest = bin_dir / fname
        if dest.exists():
            print(f"  ✓ {fname} ya existe en {dest}")
            continue
        print(f"  Descargando {fname}...")
        try:
            urllib.request.urlretrieve(url, dest)
            print(f"  ✓ {fname} descargado en {dest}")
        except Exception as e:
            print(f"  ✗ Error descargando {fname}: {e}")
            print(f"    Descarga manual en: {url}")
            print(f"    Colocar en: {dest}")

    return str(hadoop_home)

def set_env_variable_permanent(name, value):
    """Establece variable de entorno de forma permanente para el usuario."""
    try:
        subprocess.run(
            ["setx", name, value],
            check=True, capture_output=True
        )
        print(f"  ✓ Variable {name} = {value}  (permanente)")
    except Exception as e:
        print(f"  ⚠ No se pudo guardar permanentemente: {e}")
        print(f"    Agregar manualmente en: Panel de Control > Variables de entorno")
        print(f"    Variable: {name}  Valor: {value}")
    # También setear en la sesión actual
    os.environ[name] = value

def check_pyspark():
    print("\n[3/4] Verificando PySpark...")
    try:
        import pyspark
        print(f"  ✓ PySpark {pyspark.__version__} instalado")
        return True
    except ImportError:
        print("  ✗ PySpark no encontrado. Ejecutar:")
        print("    pip install pyspark==3.5.0")
        return False

def check_dependencies():
    print("\n[4/4] Verificando dependencias Python...")
    deps = {
        "pyspark":   "pyspark==3.5.0",
        "duckdb":    "duckdb>=0.10.0",
        "pyyaml":    "pyyaml>=6.0.1",
        "requests":  "requests>=2.31.0",
        "pandas":    "pandas>=2.0.0",
        "pyarrow":   "pyarrow>=12.0.0",
        "matplotlib":"matplotlib>=3.7.0",
    }
    missing = []
    for pkg, install_cmd in deps.items():
        try:
            __import__(pkg)
            print(f"  ✓ {pkg}")
        except ImportError:
            print(f"  ✗ {pkg}  → pip install {install_cmd}")
            missing.append(install_cmd)
    if missing:
        print(f"\n  Instalar todo de una vez:")
        print(f"    pip install {' '.join(missing)}")
    return len(missing) == 0

def create_env_file(hadoop_home):
    """Crea archivo .env con las variables necesarias."""
    env_content = f"""# Variables de entorno para PySpark en Windows
# Cargar con: from dotenv import load_dotenv; load_dotenv()
# O ejecutar setup_windows.py una sola vez

HADOOP_HOME={hadoop_home}
HADOOP_HOME_DIR={hadoop_home}
PYSPARK_PYTHON=python
PYSPARK_DRIVER_PYTHON=python
"""
    with open(".env.windows", "w") as f:
        f.write(env_content)
    print(f"\n  Archivo .env.windows creado con configuración.")

def patch_utils_for_windows(hadoop_home):
    """Agrega configuración automática de Windows al utils.py."""
    patch_code = f'''
# ── Parche automático Windows (generado por setup_windows.py) ──
import platform as _platform
if _platform.system() == "Windows":
    import os as _os
    _hadoop = r"{hadoop_home}"
    _os.environ.setdefault("HADOOP_HOME",     _hadoop)
    _os.environ.setdefault("hadoop.home.dir", _hadoop)
    _os.environ["PATH"] = _os.path.join(_hadoop, "bin") + ";" + _os.environ.get("PATH", "")
# ─────────────────────────────────────────────────────────────
'''
    utils_path = Path("src/utils.py")
    content = utils_path.read_text(encoding="utf-8")
    if "Parche automático Windows" not in content:
        new_content = patch_code + content
        utils_path.write_text(new_content, encoding="utf-8")
        print(f"\n  ✓ src/utils.py actualizado con soporte Windows automático")
    else:
        print(f"\n  ✓ src/utils.py ya tiene soporte Windows")

def main():
    print("=" * 60)
    print("  SETUP WINDOWS — ETL Spark Parquet Advanced")
    print("  Soluciona: HADOOP_HOME not set (WindowsProblems)")
    print("=" * 60)

    if platform.system() != "Windows":
        print("Este script es solo para Windows. En Linux/Mac no es necesario.")
        return

    java_ok   = check_java()
    hadoop_home = setup_hadoop_home()

    print("\n  Configurando variables de entorno...")
    set_env_variable_permanent("HADOOP_HOME",     hadoop_home)
    set_env_variable_permanent("hadoop.home.dir", hadoop_home)

    # Agregar bin al PATH
    bin_path = str(Path(hadoop_home) / "bin")
    current_path = os.environ.get("PATH", "")
    if bin_path not in current_path:
        set_env_variable_permanent("PATH", f"{bin_path};{current_path}")

    pyspark_ok = check_pyspark()
    deps_ok    = check_dependencies()

    create_env_file(hadoop_home)
    patch_utils_for_windows(hadoop_home)

    print("\n" + "=" * 60)
    print("  RESUMEN")
    print("=" * 60)
    print(f"  Java         : {'✓' if java_ok else '✗ PENDIENTE'}")
    print(f"  HADOOP_HOME  : ✓ {hadoop_home}")
    print(f"  PySpark      : {'✓' if pyspark_ok else '✗ pip install pyspark==3.5.0'}")
    print(f"  Dependencias : {'✓' if deps_ok else '✗ Revisar arriba'}")
    print("\n  IMPORTANTE: Reiniciar la terminal/IDE después de este script")
    print("  Luego ejecutar: python pipeline.py")
    print("=" * 60)

if __name__ == "__main__":
    main()
