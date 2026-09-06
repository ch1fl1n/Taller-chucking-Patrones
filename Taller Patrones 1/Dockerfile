# Imagen única reutilizada por scheduler, workers y prefect-runner.
# Cada servicio en docker-compose.yml sobreescribe el CMD con su propio command.

FROM python:3.11-slim

# Dependencias del sistema mínimas para pyarrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar dependencias Python primero (mejor uso de la caché de capas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY generate_dirty_data.py pipeline.py ./

# Crear directorios del Data Lake dentro del volumen compartido
RUN mkdir -p /app/shared-data/raw /app/shared-data/processed

# CMD neutro: cada servicio en compose lo sobreescribe
CMD ["python", "--version"]
