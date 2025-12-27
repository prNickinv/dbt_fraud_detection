WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

customer_stats AS (
    SELECT
        first_name,
        last_name,
        count(*) AS total_transactions,
        avg(amount) AS avg_amount,
        sum(is_fraud) AS total_fraud,
        round(sum(is_fraud) / count(*) * 100, 2) AS fraud_rate_pct
    FROM transactions
    GROUP BY first_name, last_name
),

final AS (
    SELECT
        *,
        CASE
            WHEN total_fraud > 0 OR avg_amount > 500 THEN 'HIGH'
            WHEN avg_amount > 100 AND total_transactions > 1250 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level

    FROM customer_stats
)

SELECT * FROM final
