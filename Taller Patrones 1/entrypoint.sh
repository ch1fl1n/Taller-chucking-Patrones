#!/bin/sh
set -e

# Arranca el demonio Docker interno en segundo plano
dockerd &

# Espera a que el socket interno esté listo
until docker info >/dev/null 2>&1; do
  echo "Esperando al Docker interno..."
  sleep 1
done

echo "Docker interno listo. Iniciando orquestador Dask..."
exec python3 launch_containers.py
