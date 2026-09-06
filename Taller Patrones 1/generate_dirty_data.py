"""
generate_dirty_data.py
----------------------
Genera 300.000 filas de datos sucios con 4 tipos de anomalías y los
particiona en 6 archivos CSV (~50.000 filas c/u) dentro de shared-data/raw/.

Anomalías inyectadas:
  1. raw_customer_code  — formatos heterogéneos: CLI-XXXXX / cli_XXXXX /
                          RAW#XXXXX / CUST-XXXXX / solo dígitos.
  2. city_notes_corrupted — mojibake: texto codificado en latin-1 y mal
                            decodificado como windows-1252.
  3. phone_raw          — formatos caóticos: con/sin guiones, paréntesis,
                          prefijos internacionales, strings vacíos.
  4. amount_usd         — 3 % de valores nulos (NaN).

Uso:
  python generate_dirty_data.py                  # genera en shared-data/raw/
  docker compose run --rm prefect-runner python generate_dirty_data.py
"""

import os
import numpy as np
import pandas as pd

# ── Configuración ──────────────────────────────────────────────────────────────

SEED       = 42
NUM_ROWS   = int(os.environ.get("NUM_ROWS", 300_000))
NUM_PARTS  = 6
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "shared-data", "raw")

rng = np.random.default_rng(SEED)

# ── Generadores de columnas ────────────────────────────────────────────────────

def gen_customer_code(n: int) -> np.ndarray:
    """Códigos de cliente con prefijos heterogéneos (anomalía 1)."""
    numeros = rng.integers(10_000, 99_999, size=n)
    prefijos = rng.choice(
        ["CLI-", "cli_", "RAW#", "CUST-", ""],
        size=n,
        p=[0.25, 0.20, 0.20, 0.20, 0.15],
    )
    return np.array([f"{p}{d}" for p, d in zip(prefijos, numeros)])


def gen_city_notes(n: int) -> np.ndarray:
    """
    Notas de ciudad con mojibake (anomalía 2).
    Se codifican en latin-1 y se decodifican mal como windows-1252,
    produciendo caracteres corruptos en los acentos y eñes.
    """
    ciudades = [
        "Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena",
        "Bucaramanga", "Pereira", "Manizáles", "Cúcuta", "Ibagué",
    ]
    notas_base = [
        f"Entrega en {c}, verificar dirección" for c in ciudades
    ]
    seleccion = rng.choice(notas_base, size=n)

    corrupted = []
    for texto in seleccion:
        try:
            # Simula el mojibake: latin-1 → bytes → decodifica como windows-1252
            corrupted.append(texto.encode("latin-1").decode("windows-1252"))
        except (UnicodeEncodeError, UnicodeDecodeError):
            corrupted.append(texto)  # fallback si el char no es encodeable
    return np.array(corrupted)


def gen_phone(n: int) -> np.ndarray:
    """Teléfonos en formatos caóticos (anomalía 3)."""
    base = rng.integers(3_000_000_000, 3_299_999_999, size=n).astype(str)
    formatos = rng.integers(0, 7, size=n)

    phones = []
    for num, fmt in zip(base, formatos):
        d = num  # 10 dígitos
        if fmt == 0:
            phones.append(d)                              # solo dígitos
        elif fmt == 1:
            phones.append(f"+57 {d[:3]} {d[3:6]} {d[6:]}")  # internacional
        elif fmt == 2:
            phones.append(f"({d[:3]}) {d[3:6]}-{d[6:]}")    # con paréntesis
        elif fmt == 3:
            phones.append(f"{d[:3]}-{d[3:6]}-{d[6:]}")      # guiones
        elif fmt == 4:
            phones.append(f"57{d}")                           # prefijo pegado
        elif fmt == 5:
            phones.append("")                                 # vacío
        else:
            phones.append(f"TEL:{d}")                        # prefijo texto
    return np.array(phones)


def gen_amount(n: int) -> np.ndarray:
    """Montos con 3 % de NaN (anomalía 4)."""
    valores = rng.uniform(10.0, 10_000.0, size=n).round(2)
    nulos = rng.random(size=n) < 0.03
    result = valores.astype(object)
    result[nulos] = np.nan
    return result


def gen_dates(n: int) -> np.ndarray:
    """Fechas de transacción en rango 2022-2024."""
    start = np.datetime64("2022-01-01")
    end   = np.datetime64("2024-12-31")
    dias  = (end - start).astype(int)
    offsets = rng.integers(0, dias, size=n)
    return (start + offsets.astype("timedelta64[D]")).astype(str)


def gen_product_category(n: int) -> np.ndarray:
    cats = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home"]
    return rng.choice(cats, size=n)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Generando {NUM_ROWS:,} filas con semilla {SEED}...")

    df = pd.DataFrame({
        "raw_customer_code":    gen_customer_code(NUM_ROWS),
        "city_notes_corrupted": gen_city_notes(NUM_ROWS),
        "phone_raw":            gen_phone(NUM_ROWS),
        "amount_usd":           gen_amount(NUM_ROWS),
        "transaction_date":     gen_dates(NUM_ROWS),
        "product_category":     gen_product_category(NUM_ROWS),
    })

    # Particionar en NUM_PARTS CSV para que Dask los distribuya entre workers
    particiones = np.array_split(df, NUM_PARTS)
    for i, parte in enumerate(particiones, start=1):
        ruta = os.path.join(OUTPUT_DIR, f"transactions_part_{i:02d}.csv")
        parte.to_csv(ruta, index=False)
        print(f"  Escrito: {ruta}  ({len(parte):,} filas)")

    print(f"\nDatos generados en: {OUTPUT_DIR}")
    print(f"Total filas: {NUM_ROWS:,} | Particiones: {NUM_PARTS}")


if __name__ == "__main__":
    main()
