import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from clickhouse_connect import get_client



def build_preprocessor_from_sample(
    host='localhost',
    port=8123,
    user='default',
    password='monSuperMotDePasse',
    database='default',
    table='client',
    sample_size=10000
) -> ColumnTransformer:
    client = get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )

    # Colonnes
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
    selected_columns = numeric_features + categorical_features

    # Récupération des catégories uniques
    category_values = {}
    for col in categorical_features:
        query = f"SELECT DISTINCT {col} FROM {table}"
        result = client.query(query)
        values = [row[0] for row in result.result_rows if row[0] is not None]
        category_values[col] = sorted(values)

    # Récupération de l'échantillon aléatoire
    query = f"""
    SELECT {', '.join(selected_columns)}
    FROM {table}
    ORDER BY rand()
    LIMIT {sample_size}
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    # Conversion des types numériques
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

    # Construction du préprocesseur
    scaler = StandardScaler()
    encoder = OneHotEncoder(
        categories=[category_values[col] for col in categorical_features],
        handle_unknown='ignore',
        sparse_output=False
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', scaler, numeric_features),
            ('cat', encoder, categorical_features)
        ]
    )

    # Fit sur l’échantillon
    preprocessor.fit(df)

    print(f"Préprocesseur entraîné sur un échantillon de {len(df)} lignes.")
    return preprocessor





def stream_user_metrics_by_id_range(
    preprocessor,
    start_id: int,
    end_id: int,
    host='localhost',
    port=8123,
    user='default',
    password='monSuperMotDePasse',
    database='default',
    table='client'
) -> pd.DataFrame:
    
    '''
    renvoie une portion du dataset, entre start_id et end_id
    normalisé et one hot encodé 
    '''

    from clickhouse_connect import get_client
    client = get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )

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
    selected_columns = ['user_id'] + numeric_features + categorical_features

    query = f"""
    SELECT {', '.join(selected_columns)}
    FROM {table}
    WHERE user_id BETWEEN {start_id} AND {end_id}
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    if df.empty:
        return pd.DataFrame()

    # Casts
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

    # Transformation
    X = preprocessor.transform(df[numeric_features + categorical_features])
    ohe = preprocessor.named_transformers_['cat']
    encoded_cat_cols = ohe.get_feature_names_out(categorical_features)
    all_columns = numeric_features + list(encoded_cat_cols)
    X_df = pd.DataFrame(X, columns=all_columns)

    # Ajouter user_id pour suivi
    X_df['user_id'] = df['user_id'].values

    return X_df
