import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# Connexion ClickHouse
from clickhouse_connect import get_client

def fetch_user_metrics(
    host='localhost',
    port=8123,
    user='default',
    password='monSuperMotDePasse',
    database='default',
    table='client'
) -> pd.DataFrame:
    client = get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )

    selected_columns = [
        'n_clicks', 'n_carts', 'n_purchases',
        'cart_to_click_ratio', 'purchase_to_cart_ratio', 'purchase_to_click_ratio',
        'unique_products', 'unique_categories',
        'most_common_category', 'most_common_brand',
        'n_sessions', 'avg_session_len_sec',
        'user_lifespan_days', 'product_revisit_rate',
        'total_spent', 'avg_item_value',
        'modal_hour', 'modal_weekday', 'recency_days'
    ]

    query = f"SELECT {', '.join(selected_columns)} FROM {table}"
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    # Conversion des types numériques vers float32 / int32
    float_cols = [
        'cart_to_click_ratio', 'purchase_to_cart_ratio', 'purchase_to_click_ratio',
        'avg_session_len_sec', 'user_lifespan_days',
        'product_revisit_rate', 'total_spent', 'avg_item_value', 'recency_days'
    ]
    int_cols = [
        'n_clicks', 'n_carts', 'n_purchases',
        'unique_products', 'unique_categories',
        'n_sessions', 'modal_hour', 'modal_weekday'
    ]

    df[float_cols] = df[float_cols].astype('float32')
    df[int_cols] = df[int_cols].astype('int32')

    return df


# Charger les données
df = fetch_user_metrics()

# 1.2 Colonnes à transformer
numeric_features = [
    'n_clicks', 'n_carts', 'n_purchases',
    'cart_to_click_ratio', 'purchase_to_cart_ratio', 'purchase_to_click_ratio',
    'unique_products', 'unique_categories',
    'n_sessions', 'avg_session_len_sec',
    'user_lifespan_days', 'product_revisit_rate',
    'total_spent', 'avg_item_value',
    'modal_hour', 'modal_weekday', 'recency_days'
]

categorical_features = ['most_common_category', 'most_common_brand']

# 1.3 Pipeline de prétraitement
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numeric_features),
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output= False), categorical_features)
    ]
)   

# Appliquer le pipeline
X = preprocessor.fit_transform(df)

# Récupérer les noms des colonnes encodées
ohe = preprocessor.named_transformers_['cat']
encoded_cat_cols = ohe.get_feature_names_out(categorical_features)
all_features = numeric_features + list(encoded_cat_cols)

# Reconvertir en DataFrame pour lisibilité
X_df = pd.DataFrame(X, columns=all_features)

print("Données préparées pour clustering, shape:", X_df.shape)
print(X_df.head())


