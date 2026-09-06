"""
launch_containers.py
--------------------
Orquestador Dask con Docker-in-Docker real.

Arquitectura:
  CONTENEDOR ORQUESTADOR (docker:dind + Python)
      ├── dockerd  (daemon Docker propio, aislado del host)
      ├── Dask Scheduler  (escucha en 0.0.0.0:8786)
      └── docker0 bridge interna (ej. 172.17.0.0/16)
               ├── worker-1  →  dask worker tcp://<gateway>:8786
               ├── worker-2  →  dask worker tcp://<gateway>:8786
               └── worker-3  →  dask worker tcp://<gateway>:8786

Los workers son contenedores reales creados por el dockerd INTERNO,
invisibles para el host. Se conectan al scheduler usando la IP gateway
de la bridge interna, que es la interfaz del orquestador en esa red.
"""

import docker
from dask.distributed import Client, LocalCluster


# ── Helpers ────────────────────────────────────────────────────────────────────

def obtener_gateway_bridge_interna(cliente_docker: docker.DockerClient) -> str:
    """
    Obtiene la IP gateway de la red bridge interna del dockerd anidado.
    Esa IP es alcanzable desde los contenedores hijos y apunta al proceso
    del scheduler Dask que corre en el orquestador.
    """
    red = cliente_docker.networks.get("bridge")
    return red.attrs["IPAM"]["Config"][0]["Gateway"]


def crear_worker(nombre: str, scheduler_addr: str,
                 imagen: str = "ghcr.io/dask/dask:2024.2.0") -> str:
    """
    Crea un contenedor worker Dask usando el dockerd INTERNO del orquestador.
    Esta función se ejecuta como tarea Dask, pero solo se usa para lanzar
    los contenedores; el scheduler y los futuros se gestionan en el proceso
    principal.
    """
    cliente = docker.from_env()  # se conecta al dockerd interno via /var/run/docker.sock

    # Idempotencia: elimina el worker previo si ya existe
    try:
        existente = cliente.containers.get(nombre)
        existente.remove(force=True)
        print(f"[INFO] Contenedor previo '{nombre}' eliminado.")
    except docker.errors.NotFound:
        pass

    contenedor = cliente.containers.run(
        image=imagen,
        command=f"dask worker {scheduler_addr}",
        name=nombre,
        detach=True,
        # Sin network_mode especial: usa la bridge interna del dockerd anidado
    )
    return f"Worker '{nombre}' lanzado (ID {contenedor.short_id})"


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Conectar al dockerd INTERNO (no al del host)
    cliente_docker = docker.from_env()

    # 2. Obtener la IP gateway de la bridge interna
    #    Los workers usarán esta IP para alcanzar el scheduler
    gateway_ip = obtener_gateway_bridge_interna(cliente_docker)
    scheduler_addr = f"tcp://{gateway_ip}:8786"
    print(f"Gateway bridge interna: {gateway_ip}")
    print(f"Dirección del scheduler para los workers: {scheduler_addr}\n")

    # 3. Levantar scheduler Dask en este proceso (el orquestador)
    #    n_workers=0: ningún worker local, todos serán los contenedores Docker
    #    host="0.0.0.0": escucha en todas las interfaces, incluida la bridge interna
    cluster = LocalCluster(
        n_workers=0,
        scheduler_options={"host": "0.0.0.0", "port": 8786},
        dashboard_address=":8787",
    )
    client = Client(cluster)
    print(f"Scheduler activo en {scheduler_addr}")
    print(f"Dashboard: http://localhost:8787/status\n")

    # 4. Crear los 3 contenedores worker directamente (sin submit de Dask,
    #    porque aún no hay workers registrados para ejecutar las tareas)
    nombres = ["worker-1", "worker-2", "worker-3"]
    print("Lanzando contenedores worker...")
    for nombre in nombres:
        resultado = crear_worker(nombre, scheduler_addr)
        print(f"  {resultado}")

    # 5. Esperar a que los 3 workers se registren en el scheduler
    print("\nEsperando que los workers se conecten al scheduler...")
    client.wait_for_workers(n_workers=3, timeout=120)
    print(f"Workers conectados: {len(client.scheduler_info()['workers'])}")
    print("Cluster operativo.\n")

    # 6. Ejemplo de tareas distribuidas entre los 3 workers
    def cuadrado(n: int) -> int:
        return n * n

    numeros = list(range(1, 13))  # 12 tareas → ~4 por worker
    futuros = client.map(cuadrado, numeros)
    resultados = client.gather(futuros)

    print("=== Resultados de tareas distribuidas ===")
    for n, r in zip(numeros, resultados):
        print(f"  {n}² = {r}")

    input("\nPresiona Enter para detener el clúster y limpiar contenedores...\n")

    # 7. Limpiar contenedores worker
    print("Limpiando contenedores...")
    for nombre in nombres:
        try:
            c = cliente_docker.containers.get(nombre)
            c.stop(timeout=5)
            c.remove()
            print(f"  '{nombre}' eliminado.")
        except docker.errors.NotFound:
            pass

    client.close()
    cluster.close()
    print("Listo.")
