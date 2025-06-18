from clickhouse_connect import get_client

# === Configuration de connexion ===
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'monSuperMotDePasse'  # <-- Mets ici ton mot de passe
DATABASE = 'default'
TABLE_NAME = 'events'

# Connexion au serveur ClickHouse
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD
)

# Vérifie si la table existe déjà
tables = client.query(f"SHOW TABLES FROM {DATABASE}").result_rows
table_names = [row[0] for row in tables]

if TABLE_NAME in table_names:
    print(f"✅ La table '{TABLE_NAME}' existe déjà dans la base '{DATABASE}'.")
else:
    print(f"🚧 La table '{TABLE_NAME}' n'existe pas, création en cours...")
    create_table_sql = f"""
    CREATE TABLE {TABLE_NAME} (
        event_time       DateTime64(3, 'UTC'),
        event_type       LowCardinality(String),
        product_id       UInt32,
        brand            LowCardinality(String),
        price            Float32,
        user_id          UInt64,
        user_session     UUID,
        category_code_1  LowCardinality(String),
        category_code_2  LowCardinality(String),
        category_code_3  LowCardinality(String),
        category_code_4  LowCardinality(String)
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(event_time)
    ORDER BY (event_time, user_id);
    """
    client.command(create_table_sql)
    print(f"✅ Table '{TABLE_NAME}' créée avec succès.")
