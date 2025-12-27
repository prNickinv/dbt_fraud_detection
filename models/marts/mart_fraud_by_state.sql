WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

final AS (
    SELECT
        us_state,
        count(*) AS total_transactions,
        round(sum(is_fraud) / count(*) * 100, 2) AS fraud_rate_pct,
        uniq(first_name, last_name) AS unique_customers,
        uniq(merchant_name) AS unique_merchants,
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount
    FROM transactions
    GROUP BY us_state
)

SELECT * FROM final
