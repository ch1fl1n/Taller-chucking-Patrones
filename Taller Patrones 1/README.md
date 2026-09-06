# Taller Patrones 1 — Pipeline de datos distribuido con Dask + Prefect

## Arquitectura

```
Host
└── docker compose up
    │
    ├── dask-scheduler   (puerto 8786 IPC, 8787 dashboard)
    ├── dask-worker-1    (2 threads, 1.5 GB RAM)
    ├── dask-worker-2    (2 threads, 1.5 GB RAM)
    ├── dask-worker-3    (2 threads, 1.5 GB RAM)
    └── prefect-runner   (orquestador, corre pipeline.py)
              │
              └── shared-data/        ← volumen Docker compartido
                    ├── raw/          ← 6 CSV generados (~50k filas c/u)
                    └── processed/    ← Parquet limpio (salida final)
```

Todos los servicios usan la misma imagen (`python:3.11-slim` + dependencias),
resuelven nombres por la red `dask-cluster-net`, y comparten datos vía el
volumen `shared-data` montado en `/app/shared-data`.

---

## Estructura del proyecto

```
.
├── docker-compose.yml       # Topología completa del clúster
├── Dockerfile               # Imagen única para los 5 servicios
├── requirements.txt         # Dependencias fijadas
├── generate_dirty_data.py   # Generador de datos sucios (300k filas, 4 anomalías)
├── pipeline.py              # Flujo Prefect con tareas Dask
├── entrypoint.sh            # (variante DinD) arranca dockerd interno
└── shared-data/
    ├── raw/                 # (se llena al correr el generador)
    └── processed/           # (salida Parquet del pipeline)
```

---

## Pasos de ejecución

### 1. Construir las imágenes

```bash
docker compose build
```

### 2. Generar los datos sucios

Este paso se ejecuta una sola vez antes de correr el pipeline.
Crea los 6 CSV con anomalías en `shared-data/raw/`.

```bash
docker compose run --rm prefect-runner python generate_dirty_data.py
```

Salida esperada:
```
Generando 300.000 filas con semilla 42...
  Escrito: shared-data/raw/transactions_part_01.csv  (50.000 filas)
  ...
  Escrito: shared-data/raw/transactions_part_06.csv  (50.000 filas)
```

### 3. Levantar el clúster y correr el pipeline

```bash
docker compose up
```

El orden de arranque es:
1. `dask-scheduler` (espera healthcheck en puerto 8786)
2. `dask-worker-1/2/3` (esperan que el scheduler esté healthy)
3. `prefect-runner` (espera que los workers estén started, luego conecta)

### 4. Ver el dashboard de Dask

Mientras el clúster está corriendo, abre en el navegador:

**http://localhost:8787**

Desde el dashboard puedes ver:
- Workers conectados y sus recursos
- Tareas en ejecución / completadas
- Gráfico de dependencias del DAG de Dask

### 5. Salida esperada del pipeline

```
=== Pipeline completado exitosamente ===
  Total filas procesadas : 300.000
  Anomalías customer_code: ~0     (< 1 %)
  Teléfonos sin dato     : ~43k   (~14 %)
  Nulos amount imputados : ~9k    (~3 % → 0 % tras imputación)
```

El resultado final queda en `shared-data/processed/` como archivos Parquet.

### 6. Detener el clúster

```bash
docker compose down
```

---

## Anomalías en los datos (generate_dirty_data.py)

| Columna | Anomalía | Solución en pipeline |
|---|---|---|
| `raw_customer_code` | Prefijos heterogéneos: `CLI-`, `cli_`, `RAW#`, `CUST-`, sin prefijo | Regex extrae los 5 dígitos → `CUST-XXXXX` |
| `city_notes_corrupted` | Mojibake: latin-1 mal decodificado como windows-1252 | Re-encode windows-1252 → decode utf-8 |
| `phone_raw` | Formatos caóticos: guiones, paréntesis, prefijos, vacíos | Extrae solo dígitos, valida longitud 7-15 |
| `amount_usd` | 3 % de valores nulos (NaN) | Imputa con mediana por partición |

---

## Simular tolerancia a fallos (pregunta 2 del PDF)

Para observar cómo Dask reprograma tareas cuando un worker cae:

1. Mientras el pipeline corre, abre otra terminal y ejecuta:
   ```bash
   docker compose stop dask-worker-2
   ```
2. Observa en el dashboard (`localhost:8787`) cómo el scheduler reprograma
   los chunks del worker-2 en los workers 1 y 3.
3. El pipeline termina correctamente sin intervención manual.

---

## Por qué Parquet y no CSV (pregunta 3 del PDF)

- **Compresión columnar:** Parquet almacena datos por columna, logrando
  compresiones de 5-10x vs CSV para datos tabulares.
- **Lectura selectiva:** puedes leer solo las columnas que necesitas, sin
  deserializar el resto.
- **Tipos de datos preservados:** CSV pierde tipos al serializar; Parquet
  los guarda en el schema.
- **Compatible con el ecosistema Big Data:** Spark, Hive, Athena, BigQuery
  leen Parquet nativo.

---

## Variante alternativa: Docker-in-Docker (DinD)

Existe también una variante donde un solo contenedor orquestador lanza los
3 workers dinámicamente usando su propio daemon Docker interno (`--privileged`).
Ver `entrypoint.sh` y la sección DinD del historial del proyecto. No se usa
como entregable principal porque la rúbrica pide explícitamente Docker Compose.

---

## Requisitos del host

- Docker Desktop 4.x+ (Windows/Mac) o Docker Engine 20.10+ (Linux)
- Docker Compose v2+
- ~5 GB de RAM disponibles para los 5 contenedores
