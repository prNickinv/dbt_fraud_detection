WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

final AS (
    SELECT
        category,
        count(*) AS total_transactions,
        sum(is_fraud) AS total_fraud,
        round(sum(is_fraud) / count(*) * 100, 2) AS fraud_rate_pct,
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount

    FROM transactions
    GROUP BY category
    ORDER BY fraud_rate_pct DESC
)

SELECT * FROM final
