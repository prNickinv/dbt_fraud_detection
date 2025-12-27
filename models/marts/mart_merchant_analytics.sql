WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

merchant_stats AS (
    SELECT
        merchant_name,
        count(*) AS total_transactions,
        sum(is_fraud) AS total_fraud,
        round(avg(is_fraud) * 100, 2) AS fraud_rate_pct,

        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount,
        round(
            sumIf(amount, is_fraud = 1) / sum(amount) * 100, 2
        ) AS fraud_amount_pct

    FROM transactions
    GROUP BY merchant_name
),

final AS (
    SELECT
        *,
        CASE
            WHEN fraud_amount_pct >= 5 THEN 1
            ELSE 0
        END AS is_suspicious_merchant
    FROM merchant_stats
)

SELECT * FROM final
