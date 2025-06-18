import os
import pandas as pd
from clickhouse_connect import get_client
import numpy as np

# =========== Configuration ===========
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'monSuperMotDePasse'
DATABASE = 'default'
TABLE = 'events'
CSV_FOLDER = './csv_files'

client = get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

def import_csv_to_clickhouse(csv_path):
    print(f"Insertion du fichier : {csv_path}")

    # chargement des données
    dtypes = {
        "event_type"  : "category",
        "product_id"  : "category",
        "category_id" : "int64",     # nullable
        "category_code": "category",
        "brand"       : "category",
        "price"       : "float32",
        "user_id"     : "category",
        "user_session": "category"
    }
    df = pd.read_csv(csv_path,dtype=dtypes,parse_dates=["event_time"])

    # =========== Traitement custom ===========

    df = df.drop(columns="category_id")  
    df = df.drop_duplicates(                                            #doublon
        subset=["event_time", "user_id", "product_id", "event_type"]
    )
    df = df.dropna(subset=["price","user_session"])                     #valeurs manquantes
    for col in ["brand", "category_code"]:                              #normalisation des strings
        df[col] = (
            df[col]
            .str.lower()
            .str.strip()
            .fillna("unknown")
            .replace({"": "unknown", "nan": "unknown"})
        )

    # Séparer category_code en colonnes selon les niveaux hiérarchiques
    categories_split = df['category_code'].str.split('.', expand=True)

    # Nommer dynamiquement les colonnes
    categories_split.columns = [f'category_code_{i+1}' for i in range(categories_split.shape[1])]

    # Forcer toutes les valeurs manquantes à être des np.nan
    categories_split = categories_split.where(pd.notna(categories_split), np.nan)

    # Convertir chaque colonne en type category
    for col in categories_split.columns:
        categories_split[col] = categories_split[col].astype('category')

    # Supprimer la colonne initiale et concaténer les colonnes éclatées
    df.drop(columns=['category_code'], inplace=True)
    df = pd.concat([df, categories_split], axis=1)

    # free some memory
    del categories_split
    # remplacement des valeurs vides
    df['category_code_2'] = df['category_code_2'].astype('object').fillna('unknown').astype('category')
    df['category_code_3'] = df['category_code_3'].astype('object').fillna('unknown').astype('category')
    df['category_code_4'] = df['category_code_4'].astype('object').fillna('unknown').astype('category')

    print("traitement termminé")

    # =========== Insertion ClickHouse ===========

    client.insert_df(
        table=TABLE,
        df=df,
        database=DATABASE
    )
    print(f"{len(df)} lignes insérées depuis {os.path.basename(csv_path)}")

for filename in os.listdir(CSV_FOLDER):
    if filename.endswith('.csv'):
        file_path = os.path.join(CSV_FOLDER, filename)
        try:
            import_csv_to_clickhouse(file_path)
        except Exception as e:
            print(f"Erreur lors de l'import du fichier {filename} : {e}")
