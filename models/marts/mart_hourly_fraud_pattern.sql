WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

final AS (
    SELECT
        -- ClickHouse date functions
        toDayOfWeek(transaction_dt) AS day_of_week,
        toHour(transaction_dt) AS hour_of_day,

        count(*) AS total_transactions,
        sum(is_fraud) AS total_fraud,
        round(sum(is_fraud) / count(*) * 100, 2) AS fraud_rate_pct

    FROM transactions
    GROUP BY day_of_week, hour_of_day
)

SELECT * FROM final
