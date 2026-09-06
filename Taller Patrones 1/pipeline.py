"""
pipeline.py
-----------
Orquestador Prefect + Dask para el pipeline de limpieza de datos.

Flujo de ejecución:
  1. verificar_infraestructura  — espera a que los 3 workers estén listos
  2. cargar_datos_crudos        — lee los 6 CSV con Dask (distribuido)
  3. limpiar_customer_code      — normaliza prefijos heterogéneos
  4. reparar_mojibake           — repara city_notes_corrupted
  5. normalizar_telefono        — estandariza phone_raw
  6. rellenar_nulos_amount      — imputa NaN en amount_usd
  7. escribir_resultado         — escribe Parquet en shared-data/processed/
  8. validar_quality_gate       — verifica umbrales de calidad

Cada paso es un @task de Prefect. El @flow los encadena pasando
los DataFrames Dask entre tareas (Dask ejecuta el grafo de forma lazy
y solo materializa cuando se llama .compute() o .to_parquet()).
"""

import os
import re

import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from prefect import flow, get_run_logger, task

# ── Configuración ──────────────────────────────────────────────────────────────

SCHEDULER_ADDRESS = os.environ.get(
    "DASK_SCHEDULER_ADDRESS", "tcp://localhost:8786"
)
RAW_PATH       = "/app/shared-data/raw/*.csv"
PROCESSED_PATH = "/app/shared-data/processed/"

# Umbral máximo tolerable de filas con anomalías no resueltas (15 %)
QUALITY_THRESHOLD = 0.15

# ── Expresiones regulares precompiladas ───────────────────────────────────────

RE_CUSTOMER = re.compile(r"(?:CLI-|cli_|RAW#|CUST-)?(\d{5})")
RE_PHONE    = re.compile(r"\d")   # detectar si hay algún dígito


# ── Funciones auxiliares (se ejecutan dentro de map_partitions) ───────────────

def _normalizar_codigo(valor: str) -> str:
    """Extrae los 5 dígitos del código de cliente y devuelve CUST-XXXXX."""
    if pd.isna(valor):
        return "CUST-00000-ANOMALY"
    match = RE_CUSTOMER.search(str(valor))
    if match:
        return f"CUST-{match.group(1)}"
    return "CUST-00000-ANOMALY"


def _reparar_texto(valor: str) -> str:
    """Intenta revertir el mojibake latin-1 → windows-1252."""
    if pd.isna(valor):
        return valor
    try:
        return str(valor).encode("windows-1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        try:
            return str(valor).encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return str(valor)   # devuelve el valor original si no se puede reparar


def _normalizar_tel(valor: str) -> str:
    """Extrae dígitos del teléfono y valida longitud (7-15 dígitos)."""
    if pd.isna(valor):
        return "SIN_DATO"
    digitos = "".join(RE_PHONE.findall(str(valor)))
    if 7 <= len(digitos) <= 15:
        return digitos
    return "SIN_DATO"


def _limpiar_chunk_customer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["customer_code"] = df["raw_customer_code"].apply(_normalizar_codigo)
    return df


def _limpiar_chunk_mojibake(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["city_notes"] = df["city_notes_corrupted"].apply(_reparar_texto)
    return df


def _limpiar_chunk_telefono(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["phone"] = df["phone_raw"].apply(_normalizar_tel)
    return df


def _limpiar_chunk_amount(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mediana = df["amount_usd"].median()
    df["amount_usd"] = df["amount_usd"].fillna(mediana)
    return df


# ── Tareas Prefect ─────────────────────────────────────────────────────────────

@task(name="verificar_infraestructura", retries=3, retry_delay_seconds=10)
def verificar_infraestructura() -> Client:
    """
    Conecta al scheduler Dask y espera a que los 3 workers estén registrados.
    Prefect reintentará hasta 3 veces si el scheduler no está disponible aún.
    """
    logger = get_run_logger()
    logger.info(f"Conectando al scheduler: {SCHEDULER_ADDRESS}")
    client = Client(SCHEDULER_ADDRESS)
    client.wait_for_workers(n_workers=3, timeout=120)
    info = client.scheduler_info()
    logger.info(f"Workers conectados: {len(info['workers'])}")
    return client


@task(name="cargar_datos_crudos")
def cargar_datos_crudos() -> dd.DataFrame:
    """
    Lee los 6 CSV con Dask. Dask crea una partición por archivo,
    distribuyendo la carga entre los 3 workers automáticamente.
    """
    logger = get_run_logger()
    logger.info(f"Leyendo CSV desde: {RAW_PATH}")
    ddf = dd.read_csv(RAW_PATH, dtype={"phone_raw": "str", "amount_usd": "float64"})
    logger.info(f"Particiones: {ddf.npartitions}")
    return ddf


@task(name="limpiar_customer_code")
def limpiar_customer_code(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Normaliza raw_customer_code a formato CUST-XXXXX.
    Usa map_partitions para aplicar la lógica con regex en cada chunk.
    Los valores irrecuperables quedan como CUST-00000-ANOMALY.
    """
    logger = get_run_logger()
    logger.info("Normalizando customer_code...")
    meta = ddf._meta.copy()
    meta["customer_code"] = pd.Series(dtype="str")
    ddf = ddf.map_partitions(_limpiar_chunk_customer, meta=meta)
    return ddf


@task(name="reparar_mojibake")
def reparar_mojibake(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Repara la columna city_notes_corrupted intentando revertir
    el mojibake windows-1252 → utf-8.
    """
    logger = get_run_logger()
    logger.info("Reparando mojibake en city_notes...")
    meta = ddf._meta.copy()
    meta["city_notes"] = pd.Series(dtype="str")
    ddf = ddf.map_partitions(_limpiar_chunk_mojibake, meta=meta)
    return ddf


@task(name="normalizar_telefono")
def normalizar_telefono(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Extrae solo dígitos de phone_raw. Si el resultado no tiene
    entre 7 y 15 dígitos, marca la celda como SIN_DATO.
    """
    logger = get_run_logger()
    logger.info("Normalizando teléfonos...")
    meta = ddf._meta.copy()
    meta["phone"] = pd.Series(dtype="str")
    ddf = ddf.map_partitions(_limpiar_chunk_telefono, meta=meta)
    return ddf


@task(name="rellenar_nulos_amount")
def rellenar_nulos_amount(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Imputa los NaN de amount_usd con la mediana de cada partición.
    (Imputación local, suficiente para el ejercicio.)
    """
    logger = get_run_logger()
    logger.info("Imputando nulos en amount_usd...")
    ddf = ddf.map_partitions(_limpiar_chunk_amount)
    return ddf


@task(name="escribir_resultado")
def escribir_resultado(ddf: dd.DataFrame) -> None:
    """
    Escribe el resultado final en formato Parquet particionado.
    Parquet ofrece compresión columnar y lectura selectiva de columnas,
    lo que lo hace preferible a CSV para un Data Lake.
    """
    logger = get_run_logger()

    # Seleccionar solo las columnas limpias (descartar las originales sucias)
    columnas_finales = [
        "customer_code",
        "city_notes",
        "phone",
        "amount_usd",
        "transaction_date",
        "product_category",
    ]
    ddf_final = ddf[columnas_finales]

    logger.info(f"Escribiendo Parquet en: {PROCESSED_PATH}")
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    ddf_final.to_parquet(
        PROCESSED_PATH,
        engine="pyarrow",
        write_index=False,
        overwrite=True,
    )
    logger.info("Escritura completada.")


@task(name="validar_quality_gate")
def validar_quality_gate(ddf: dd.DataFrame) -> dict:
    """
    Verifica umbrales de calidad tras la limpieza.
    Lanza ValueError si la tasa de anomalías supera QUALITY_THRESHOLD.
    """
    logger = get_run_logger()
    logger.info("Ejecutando quality gate...")

    total = len(ddf)

    # Materializamos solo las columnas necesarias para el gate
    anomalias_cliente = (
        ddf["customer_code"].eq("CUST-00000-ANOMALY").sum().compute()
    )
    anomalias_tel = (
        ddf["phone"].eq("SIN_DATO").sum().compute()
    )
    nulos_amount = ddf["amount_usd"].isna().sum().compute()

    tasa_cliente = anomalias_cliente / total
    tasa_tel     = anomalias_tel / total
    tasa_amount  = nulos_amount / total

    reporte = {
        "total_filas":        total,
        "anomalias_cliente":  int(anomalias_cliente),
        "tasa_cliente":       round(float(tasa_cliente), 4),
        "anomalias_tel":      int(anomalias_tel),
        "tasa_tel":           round(float(tasa_tel), 4),
        "nulos_amount":       int(nulos_amount),
        "tasa_amount":        round(float(tasa_amount), 4),
    }

    logger.info(f"Reporte de calidad: {reporte}")

    for metrica, tasa in [
        ("customer_code", tasa_cliente),
        ("phone", tasa_tel),
        ("amount_usd", tasa_amount),
    ]:
        if tasa > QUALITY_THRESHOLD:
            raise ValueError(
                f"Quality gate FALLIDO: columna '{metrica}' tiene "
                f"{tasa:.1%} de anomalías (umbral: {QUALITY_THRESHOLD:.0%})"
            )

    logger.info("Quality gate PASADO.")
    return reporte


# ── Flujo principal ────────────────────────────────────────────────────────────

@flow(name="pipeline_limpieza_datos", log_prints=True)
def pipeline_completo() -> None:
    """
    Flujo Prefect que orquesta el pipeline completo de limpieza de datos
    distribuido en el clúster Dask (1 scheduler + 3 workers).
    """
    # 1. Verificar que el clúster está listo
    client = verificar_infraestructura()

    # 2. Cargar datos crudos (lazy, Dask no lee nada aún)
    ddf = cargar_datos_crudos()

    # 3. Limpieza encadenada — cada tarea devuelve un nuevo DDF transformado
    ddf = limpiar_customer_code(ddf)
    ddf = reparar_mojibake(ddf)
    ddf = normalizar_telefono(ddf)
    ddf = rellenar_nulos_amount(ddf)

    # 4. Escribir resultado (aquí Dask materializa todo el grafo)
    escribir_resultado(ddf)

    # 5. Validar calidad sobre los datos ya limpios
    reporte = validar_quality_gate(ddf)

    print("\n=== Pipeline completado exitosamente ===")
    print(f"  Total filas procesadas : {reporte['total_filas']:,}")
    print(f"  Anomalías customer_code: {reporte['anomalias_cliente']:,} "
          f"({reporte['tasa_cliente']:.2%})")
    print(f"  Teléfonos sin dato     : {reporte['anomalias_tel']:,} "
          f"({reporte['tasa_tel']:.2%})")
    print(f"  Nulos amount imputados : {reporte['nulos_amount']:,} "
          f"({reporte['tasa_amount']:.2%})")

    client.close()


if __name__ == "__main__":
    pipeline_completo()
