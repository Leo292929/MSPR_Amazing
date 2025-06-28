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
    row_number() OVER () - 1 AS user_id,  -- Commence à 0, mets +1 si tu veux commencer à 1

    n_clicks,
    n_carts,
    n_purchases,
    cart_to_click_ratio,
    purchase_to_cart_ratio,
    purchase_to_click_ratio,
    unique_products,
    unique_categories,
    most_common_category,
    most_common_brand,
    n_sessions,
    total_spent,
    avg_session_len_sec,
    user_lifespan_days,
    avg_decision_delay_sec,
    product_revisit_rate,
    avg_item_value,
    modal_hour,
    modal_weekday,
    recency_days,
    first_event,
    last_event

FROM (
    SELECT
        user_id,

        countIf(event_type = 'view') AS n_clicks,
        countIf(event_type = 'cart') AS n_carts,
        countIf(event_type = 'purchase') AS n_purchases,

        n_carts / nullIf(n_clicks, 0) AS cart_to_click_ratio,
        n_purchases / nullIf(n_carts, 0) AS purchase_to_cart_ratio,
        n_purchases / nullIf(n_clicks, 0) AS purchase_to_click_ratio,

        uniqExact(product_id) AS unique_products,
        uniqExact(category_code_1_clean) AS unique_categories,

        argMax(category_code_1_clean, category_count) AS most_common_category,
        argMax(brand_clean, brand_count) AS most_common_brand,

        uniqExact(user_session) AS n_sessions,

        sumIf(price, event_type = 'purchase') AS total_spent,

        avg(session_end - session_start) AS avg_session_len_sec,

        dateDiff('day', min(event_time), max(event_time)) AS user_lifespan_days,

        avgIf(
            dateDiff('second', cart_time, purchase_time),
            cart_time IS NOT NULL AND purchase_time IS NOT NULL
        ) AS avg_decision_delay_sec,

        sumIf(views_per_product > 1, event_type = 'view') / nullIf(countIf(event_type = 'view'), 0) AS product_revisit_rate,

        avgIf(price, event_type = 'purchase') AS avg_item_value,

        argMax(hour, hour_count) AS modal_hour,
        argMax(weekday, weekday_count) AS modal_weekday,

        dateDiff('day', max(event_time), today()) AS recency_days,

        min(event_time) AS first_event,
        max(event_time) AS last_event

    FROM (
        SELECT
            *,
            toHour(event_time) AS hour,
            toDayOfWeek(event_time) AS weekday,

            if(
                brand IN (
                    SELECT brand
                    FROM (
                        SELECT brand
                        FROM events
                        GROUP BY brand
                        ORDER BY COUNT(*) DESC
                        LIMIT 50
                    )
                ), brand, 'Other') AS brand_clean,

            if(
                category_code_1 IN (
                    SELECT category_code_1
                    FROM (
                        SELECT category_code_1
                        FROM events
                        GROUP BY category_code_1
                        ORDER BY COUNT(*) DESC
                        LIMIT 20
                    )
                ), category_code_1, 'Other') AS category_code_1_clean,

            count() OVER (PARTITION BY user_id, category_code_1_clean) AS category_count,
            count() OVER (PARTITION BY user_id, brand_clean) AS brand_count,
            count() OVER (PARTITION BY user_id, hour) AS hour_count,
            count() OVER (PARTITION BY user_id, weekday) AS weekday_count,

            minIf(event_time, event_type = 'cart') OVER (PARTITION BY user_id, product_id) AS cart_time,
            minIf(event_time, event_type = 'purchase') OVER (PARTITION BY user_id, product_id) AS purchase_time,
            countIf(event_type = 'view') OVER (PARTITION BY user_id, product_id) AS views_per_product,

            min(event_time) OVER (PARTITION BY user_id, user_session) AS session_start,
            max(event_time) OVER (PARTITION BY user_id, user_session) AS session_end
        FROM events
    )
    GROUP BY user_id
)


"""

# =========== Exécution =========== #
try:
    client.command(sql)
    print("✅ Calcul des métriques utilisateur terminé avec succès et inséré dans 'client'.")
except Exception as e:
    print(f"❌ Erreur lors de l'exécution du SQL : {e}")


