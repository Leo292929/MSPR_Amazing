import pandas as pd
from clickhouse_connect import get_client

def fetch_user_metrics(
    host='localhost',
    port=8123,
    user='default',
    password='monSuperMotDePasse',
    database='default',
    table='client'
) -> pd.DataFrame:
    """
    Récupère tout le contenu de la table user_metrics dans un DataFrame pandas.
    """
    client = get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )

    query = f"SELECT * FROM {table}"
    result = client.query(query)

    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df

df_metrics = fetch_user_metrics()
print(df_metrics.head())