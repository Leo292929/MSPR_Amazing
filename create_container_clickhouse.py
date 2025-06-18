import docker

# === Paramètres de configuration ===
container_name = "clickhouse-server"
image_name = "clickhouse/clickhouse-server"
clickhouse_port_http = 8123
clickhouse_port_tcp = 9000
clickhouse_user = "default"
clickhouse_password = "monSuperMotDePasse"  # Change ici si tu veux

# === Création du client Docker ===
client = docker.from_env()

# === Vérifie si le conteneur existe déjà ===
try:
    existing = client.containers.get(container_name)
    print(f"Le conteneur '{container_name}' existe déjà. Suppression...")
    existing.remove(force=True)
except docker.errors.NotFound:
    pass

# === Création et démarrage du conteneur ===
print(f"Lancement du conteneur '{container_name}'...")
container = client.containers.run(
    image_name,
    name=container_name,
    detach=True,
    ports={
        f"{clickhouse_port_http}/tcp": clickhouse_port_http,
        f"{clickhouse_port_tcp}/tcp": clickhouse_port_tcp,
    },
    environment={
        "CLICKHOUSE_USER": clickhouse_user,
        "CLICKHOUSE_PASSWORD": clickhouse_password
    },
    ulimits=[docker.types.Ulimit(name='nofile', soft=262144, hard=262144)],
)

print("Conteneur lancé avec succès !")
print(f"Accès HTTP : http://localhost:{clickhouse_port_http}")
