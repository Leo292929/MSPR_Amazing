import os
from clickhouse_connect import get_client

# =========== Configuration =========== #
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'monSuperMotDePasse'
DATABASE = 'default'

# Connexion ClickHouse
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE
)

# =========== SQL de calcul et insertion des métriques utilisateur =========== #
sql = """
INSERT INTO client
SELECT
    user_id,
    countIf(event_type = 'view') AS n_clicks,
    countIf(event_type = 'cart') AS n_carts,
    countIf(event_type = 'purchase') AS n_purchases,

    n_carts / nullIf(n_clicks, 0) AS cart_to_click_ratio,
    n_purchases / nullIf(n_carts, 0) AS purchase_to_cart_ratio,
    n_purchases / nullIf(n_clicks, 0) AS purchase_to_click_ratio,

    uniqExact(product_id) AS unique_products,
    uniqExact(category_code_1) AS unique_categories,

    mode(category_code_1) AS most_common_category,
    mode(brand) AS most_common_brand,

    uniqExact(user_session) AS n_sessions,

    sumIf(price, event_type = 'purchase') AS total_spent,

    avg(session_end - session_start) AS avg_session_len_sec,

    dateDiff('day', min(event_time), max(event_time)) AS user_lifespan_days,

    avgIf(
        dateDiff('second', cart_time, purchase_time),
        cart_time IS NOT NULL AND purchase_time IS NOT NULL
    ) AS avg_decision_delay_sec,

    countIf(repeat_view = 1) / nullIf(countIf(event_type = 'view'), 0) AS product_revisit_rate,

    avgIf(price, event_type = 'purchase') AS avg_item_value,

    mode(toHour(event_time)) AS modal_hour,
    mode(toDayOfWeek(event_time)) AS modal_weekday,

    dateDiff('day', max(event_time), today()) AS recency_days,

    min(event_time) AS first_event,
    max(event_time) AS last_event

FROM (
    SELECT
        *,
        minIf(event_time, event_type = 'cart') OVER (PARTITION BY user_id, product_id) AS cart_time,
        minIf(event_time, event_type = 'purchase') OVER (PARTITION BY user_id, product_id) AS purchase_time,
        lag(product_id) OVER (PARTITION BY user_id ORDER BY event_time) = product_id AS repeat_view,
        min(event_time) OVER (PARTITION BY user_id, user_session) AS session_start,
        max(event_time) OVER (PARTITION BY user_id, user_session) AS session_end
    FROM events
)
GROUP BY user_id;
"""

# =========== Exécution =========== #
try:
    client.command(sql)
    print("✅ Calcul des métriques utilisateur terminé avec succès et inséré dans 'client'.")
except Exception as e:
    print(f"❌ Erreur lors de l'exécution du SQL : {e}")